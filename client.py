import socket
import rdt
import hashlib

HOST = '192.168.1.7'
PORT = 2121

data_server_ip = None
data_server_port = None

# Initialize Active Mode state variables
is_active_mode = False
client_active_udp = None
transfer_type = "I"
transfer_mode = "S"

client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
print(f"[*] Connecting to {HOST}:{PORT}...")
client_socket.connect((HOST, PORT))

# Read welcome message
response = client_socket.recv(1024)
print(response.decode('utf-8').strip())

while True:
    user_input = input("FTP> ")
    if not user_input:
        continue

    if user_input.upper() == "PORT":
        # 1. Create client's local UDP socket for Active Mode
        if client_active_udp:
            client_active_udp.close()
        client_active_udp = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        client_active_udp.bind((HOST, 0))
        _, local_port = client_active_udp.getsockname()
        
        # 2. Format the numbers
        ip_parts = HOST.split('.')
        p1, p2 = local_port // 256, local_port % 256
        
        # 3. Send command to server
        port_cmd = f"PORT {ip_parts[0]},{ip_parts[1]},{ip_parts[2]},{ip_parts[3]},{p1},{p2}\r\n"
        client_socket.sendall(port_cmd.encode('utf-8'))
        
        is_active_mode = True
        data_server_port = None # Clear PASV state
        
        print(f"[Client Internal] -> Active UDP Data Channel listening on port {local_port}")
        print(client_socket.recv(1024).decode('utf-8').strip()) # Read 200 OK
        continue
    else:
        # Intercept TYPE and MODE to keep local state
        if user_input.upper().startswith("TYPE"):
            parts = user_input.split(" ", 1)
            if len(parts) > 1 and parts[1].upper() in ["A", "I"]:
                transfer_type = parts[1].upper()
                
        if user_input.upper().startswith("MODE"):
            parts = user_input.split(" ", 1)
            if len(parts) > 1 and parts[1].upper() in ["S", "B", "C"]:
                transfer_mode = parts[1].upper()
                
        # --- NEW LOCAL VALIDATION ---
        is_data_cmd = (user_input.upper().startswith("LIST") or 
                       user_input.upper().startswith("NLST") or 
                       user_input.upper().startswith("RETR") or 
                       user_input.upper().startswith("STOR") or 
                       user_input.upper().startswith("STOU"))
        if is_data_cmd and not data_server_port and not is_active_mode:
            print(f"Error: You must send PASV or PORT before {user_input.upper().split(' ')[0]}.")
            continue
                
        # Send any other command normally
        client_socket.sendall((user_input + "\r\n").encode('utf-8'))
    
    if user_input.upper().startswith("LIST") or user_input.upper().startswith("NLST"):
        print(client_socket.recv(1024).decode('utf-8').strip())
        
        if is_active_mode:
            # Active Mode: Wait for the server to send the data to our open port
            udp_data, _ = client_active_udp.recvfrom(4096)
        else:
            # Passive Mode: Create a socket, knock, and listen
            client_udp = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            client_udp.sendto(b"KNOCK", (data_server_ip, data_server_port))
            udp_data, _ = client_udp.recvfrom(4096)
            client_udp.close()
            data_server_port = None
            
        print("\n--- DIRECTORY LISTING ---")
        print(udp_data.decode('utf-8').strip())
        print("-------------------------\n")
        
        print(client_socket.recv(1024).decode('utf-8').strip())
        continue

    elif user_input.upper().startswith("RETR"):
        reply = client_socket.recv(1024).decode('utf-8').strip()
        print(reply)
        
        if not reply.startswith("150"):
            continue
            
        filename = user_input.split(" ", 1)[1]
        print(f"\n[*] Downloading {filename}...")
        
        if is_active_mode:
            # Active Mode: Wait for the server to knock so we know its address!
            _, server_active_addr = client_active_udp.recvfrom(1024)
            rdt.gbn_receive_file("downloaded_" + filename, client_active_udp, transfer_type=transfer_type, transfer_mode=transfer_mode)
        else:
            # Passive Mode: Create socket, knock, and listen
            pasv_udp = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            pasv_udp.sendto(b"KNOCK", (data_server_ip, data_server_port))
            rdt.gbn_receive_file("downloaded_" + filename, pasv_udp, transfer_type=transfer_type, transfer_mode=transfer_mode)
            pasv_udp.close()
            data_server_port = None
                
        print(f"[*] Successfully saved as downloaded_{filename}\n")

        transfer_complete_msg = client_socket.recv(1024).decode('utf-8').strip()
        print(transfer_complete_msg)

        print("[*] Requesting server hash for integrity check...")
        client_socket.sendall((f"HASH {filename}\r\n").encode('utf-8'))
        hash_reply = client_socket.recv(1024).decode('utf-8').strip()
        
        if hash_reply.startswith("213"):
            server_hash = hash_reply.split(" ")[1]
            
            # Calculate local hash of the downloaded file
            hasher = hashlib.sha256()
            with open("downloaded_" + filename, "rb") as f:
                while True:
                    chunk = f.read(4096)
                    if not chunk:
                        break
                    hasher.update(chunk)
            local_hash = hasher.hexdigest()
            
            print(f"Server SHA-256: {server_hash}")
            print(f"Local  SHA-256: {local_hash}")
            
            if server_hash == local_hash:
                print("[+] VERIFIED: File transferred perfectly without corruption!")
            else:
                print("[-] WARNING: File hashes do not match. Corruption detected!")
        else:
            print(f"[-] Could not verify hash: {hash_reply}")

        continue

    elif user_input.upper().startswith("STOR") or user_input.upper().startswith("STOU"):
        reply = client_socket.recv(1024).decode('utf-8').strip()
        print(reply)
        
        if not reply.startswith("150"):
            continue
            
        filename = user_input.split(" ", 1)[1]
        print(f"\n[*] Uploading {filename}...")
        
        try:
            if is_active_mode:
                # Active Mode: Wait for the server to knock, then send to that address
                _, server_active_addr = client_active_udp.recvfrom(1024)
                rdt.gbn_send_file(filename, client_active_udp, server_active_addr, transfer_type=transfer_type, transfer_mode=transfer_mode)
            else:
                # Passive Mode
                pasv_udp = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                rdt.gbn_send_file(filename, pasv_udp, (data_server_ip, data_server_port), transfer_type=transfer_type, transfer_mode=transfer_mode)
                pasv_udp.close()
                data_server_port = None
                
            print(f"[*] Upload complete!\n")
            
        except FileNotFoundError:
            print(f"Error: The file '{filename}' does not exist on your computer.")
            fin_pkt = rdt.make_packet(0, 0, rdt.FLAG_FIN)
            if is_active_mode:
                _, server_active_addr = client_active_udp.recvfrom(1024)
                client_active_udp.sendto(fin_pkt, server_active_addr)
            else:
                pasv_udp = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                pasv_udp.sendto(fin_pkt, (data_server_ip, data_server_port))
                pasv_udp.close()
            
        print(client_socket.recv(1024).decode('utf-8').strip())
        continue

    # Normal TCP response handling
    server_reply = client_socket.recv(1024).decode('utf-8').strip()
    print(server_reply)
    
    if server_reply.startswith("227"):
        # Reset Active Mode if they switch back to PASV
        is_active_mode = False
        if client_active_udp:
            client_active_udp.close()
            client_active_udp = None
            
        # Extract PASV Port details...
        start = server_reply.find('(') + 1
        end = server_reply.find(')')
        numbers = server_reply[start:end].split(',')
        data_server_ip = f"{numbers[0]}.{numbers[1]}.{numbers[2]}.{numbers[3]}"
        if data_server_ip == '0.0.0.0' or data_server_ip == '127.0.0.1':
            data_server_ip = HOST
        p1, p2 = int(numbers[4]), int(numbers[5])
        data_server_port = (p1 * 256) + p2
        print(f"[Client Internal] -> UDP Data Channel ready on port {data_server_port}")

    if user_input.upper() == "QUIT":
        break

client_socket.close()
print("[*] Disconnected.")