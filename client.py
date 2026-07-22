import socket
import rdt
import hashlib

HOST = '127.0.0.1'
PORT = 2121

data_server_ip = None
data_server_port = None

client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
print(f"[*] Connecting to {HOST}:{PORT}...")
client_socket.connect((HOST, PORT))

# Read welcome message
response = client_socket.recv(1024)
print(response.decode('utf-8').strip())

while True:
    # Get text input from you in the terminal
    user_input = input("FTP> ")
    if not user_input:
        continue
        
    # Send to server
    # Example for PASV:
    # If the user types "PASV", the client sends over TCP: b"PASV\r\n"
    client_socket.sendall((user_input + "\r\n").encode('utf-8'))
    
    if user_input.upper() == "LIST":
        if not data_server_port:
            print("Error: You must send PASV before LIST.")
            # Still need to read the server's 425 error over TCP
            print(client_socket.recv(1024).decode('utf-8').strip())
            continue
            
        # 1. Read the 150 start message from TCP
        print(client_socket.recv(1024).decode('utf-8').strip())
        
        # 2. Create our UDP socket
        client_udp = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        
        # 3. Send a "knock" to the server so it knows our address
        client_udp.sendto(b"KNOCK", (data_server_ip, data_server_port))
        
        # 4. Wait for the server to blast the directory data over UDP
        udp_data, _ = client_udp.recvfrom(4096)
        print("\n--- DIRECTORY LISTING ---")
        print(udp_data.decode('utf-8').strip())
        print("-------------------------\n")
        
        # 5. Clean up UDP and read the 226 complete message from TCP
        client_udp.close()
        data_server_port = None # Reset for next time
        print(client_socket.recv(1024).decode('utf-8').strip())
        continue

    elif user_input.upper().startswith("RETR"):
        if not data_server_port:
            print("Error: You must send PASV before RETR.")
            print(client_socket.recv(1024).decode('utf-8').strip())
            continue
            
        reply = client_socket.recv(1024).decode('utf-8').strip()
        print(reply)
        
        if not reply.startswith("150"):
            continue
            
        filename = user_input.split(" ", 1)[1]
        
        client_udp = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        client_udp.sendto(b"KNOCK", (data_server_ip, data_server_port))
        
        print(f"\n[*] Downloading {filename}...")
        
        # --- NEW: Use Custom RDT to receive the file ---
        rdt.gbn_receive_file("downloaded_" + filename, client_udp)
                
        print(f"[*] Successfully saved as downloaded_{filename}\n")
        
        client_udp.close()

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

        data_server_port = None
        continue

    elif user_input.upper().startswith("STOR"):
        if not data_server_port:
            print("Error: You must send PASV before STOR.")
            print(client_socket.recv(1024).decode('utf-8').strip())
            continue
            
        reply = client_socket.recv(1024).decode('utf-8').strip()
        print(reply)
        
        if not reply.startswith("150"):
            continue
            
        filename = user_input.split(" ", 1)[1]
        
        client_udp = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        print(f"\n[*] Uploading {filename}...")
        
        try:
            # --- NEW: Use Custom RDT to send the file ---
            rdt.gbn_send_file(filename, client_udp, (data_server_ip, data_server_port))
            print(f"[*] Upload complete!\n")
            
        except FileNotFoundError:
            print(f"Error: The file '{filename}' does not exist on your computer.")
            # Send FIN anyway to prevent server from freezing
            fin_pkt = rdt.make_packet(0, 0, rdt.FLAG_FIN)
            client_udp.sendto(fin_pkt, (data_server_ip, data_server_port))
            
        client_udp.close()
        data_server_port = None
        print(client_socket.recv(1024).decode('utf-8').strip())
        continue

    # Normal TCP response handling
    server_reply = client_socket.recv(1024).decode('utf-8').strip()
    print(server_reply)
    
    # NEW: Catch the PASV response to calculate the server's UDP port
    # Example for PASV:
    # The server replies over TCP with the IP and Port for the data channel:
    # e.g., "227 Entering Passive Mode (127,0,0,1,192,52).\r\n"
    # Port is calculated as 192 * 256 + 52 = 49204.
    if server_reply.startswith("227"):
        # Extract the text between parentheses
        start = server_reply.find('(') + 1
        end = server_reply.find(')')
        numbers = server_reply[start:end].split(',')
        
        # Reconstruct IP and Port
        data_server_ip = f"{numbers[0]}.{numbers[1]}.{numbers[2]}.{numbers[3]}"
        p1, p2 = int(numbers[4]), int(numbers[5])
        data_server_port = (p1 * 256) + p2
        print(f"[Client Internal] -> UDP Data Channel ready on port {data_server_port}")

    # If we sent QUIT, break out of our local loop 
    if user_input.upper() == "QUIT":
        break

client_socket.close()
print("[*] Disconnected.")