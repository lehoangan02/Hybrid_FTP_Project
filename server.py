import socket
import os
import rdt
import hashlib
import threading

HOST = '127.0.0.1'
PORT = 2121

def handle_client(client_conn, client_addr):
    """Handles an isolated session for a single connected client."""
    print(f"\n[*] New client connected from {client_addr}")
    
    # Thread-local state: Each client tracks its own directory and data channel
    current_dir = os.getcwd()
    data_socket = None

    is_pasv = False
    client_data_addr = None
    
    try:
        # Send standard FTP welcome code
        client_conn.sendall(b"220 Service ready for new user.\r\n")
        
        while True:
            data = client_conn.recv(1024)
            if not data:
                break # Client disconnected abruptly
                
            command = data.decode('utf-8').strip()
            # Adding the port to the print statement to identify which client is talking
            print(f"[Client {client_addr[1]} says]: {command}")
            
            if command.upper().startswith("USER"):
                client_conn.sendall(b"331 Username OK, need password.\r\n")
                
            elif command.upper().startswith("PASS"):
                client_conn.sendall(b"230 User logged in successfully.\r\n")
            
            elif command.upper() == "PWD":
                reply = f"257 \"{current_dir}\" is current directory.\r\n"
                client_conn.sendall(reply.encode('utf-8'))
                
            elif command.upper().startswith("CWD"):
                parts = command.split(" ", 1)
                
                if len(parts) > 1:
                    target_dir = parts[1]
                    # FIX: Safely calculate new path without changing global server OS state
                    potential_dir = os.path.abspath(os.path.join(current_dir, target_dir))
                    
                    if os.path.isdir(potential_dir):
                        current_dir = potential_dir 
                        client_conn.sendall(b"250 Requested file action OK.\r\n")
                    else:
                        client_conn.sendall(b"550 File unavailable or not found.\r\n")
                else:
                    client_conn.sendall(b"501 Syntax error in parameters.\r\n")
            
            elif command.upper() == "PASV":
                data_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                data_socket.bind((HOST, 0))
                
                _, data_port = data_socket.getsockname()
                ip_parts = HOST.split('.') 
                
                p1 = data_port // 256
                p2 = data_port % 256
                
                reply = f"227 Entering Passive Mode ({ip_parts[0]},{ip_parts[1]},{ip_parts[2]},{ip_parts[3]},{p1},{p2}).\r\n"
                client_conn.sendall(reply.encode('utf-8'))
                print(f"[*] Prepared Passive UDP Data Channel on port {data_port}")
            
            elif command.upper() == "LIST":
                if not data_socket and not client_data_addr:
                    client_conn.sendall(b"425 Use PASV or PORT first.\r\n")
                    continue
                
                client_conn.sendall(b"150 Here comes the directory listing.\r\n")
                
                files = os.listdir(current_dir)
                file_list_str = "\n".join(files) + "\n"
                if not files:
                    file_list_str = "(Empty Directory)\n"

                if is_pasv:
                    print("[*] Waiting for client UDP knock...")
                    _, client_udp_addr = data_socket.recvfrom(1024)
                    data_socket.sendto(file_list_str.encode('utf-8'), client_udp_addr)
                    data_socket.close()
                    data_socket = None
                else:
                    active_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                    active_socket.sendto(file_list_str.encode('utf-8'), client_data_addr)
                    active_socket.close()
                
                client_conn.sendall(b"226 Directory send OK.\r\n")

            elif command.upper().startswith("PORT"):
                # E.g., PORT 127,0,0,1,192,52
                parts = command.split(" ", 1)[1].split(',')
                ip = f"{parts[0]}.{parts[1]}.{parts[2]}.{parts[3]}"
                port = (int(parts[4]) * 256) + int(parts[5])
                
                # Set Active mode flags
                client_data_addr = (ip, port)
                is_pasv = False
                
                if data_socket:
                    data_socket.close()
                    data_socket = None
                    
                client_conn.sendall(b"200 PORT command successful.\r\n")
                print(f"[*] Client requested Active Mode. Will connect to {ip}:{port}")

            elif command.upper().startswith("RETR"):
                parts = command.split(" ", 1)
                if len(parts) < 2:
                    client_conn.sendall(b"501 Syntax error in parameters.\r\n")
                    continue
                
                filename = parts[1]
                filepath = os.path.join(current_dir, filename)
                
                if not os.path.isfile(filepath):
                    client_conn.sendall(b"550 File not found.\r\n")
                    continue
                    
                if not is_pasv and not client_data_addr:
                    client_conn.sendall(b"425 Use PASV or PORT first.\r\n")
                    continue
                
                client_conn.sendall(b"150 Opening data connection for file transfer.\r\n")
                
                if is_pasv:
                    print(f"[*] Waiting for client knock to send {filename}...")
                    _, client_udp_addr = data_socket.recvfrom(1024)
                    rdt.gbn_send_file(filepath, data_socket, client_udp_addr)
                    data_socket.close()
                    data_socket = None
                else:
                    # Active Mode RETR: Server creates socket, knocks, and sends directly
                    active_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                    active_socket.bind((HOST, 0)) # Bind so we have a return address for ACKs!
                    active_socket.sendto(b"KNOCK", client_data_addr) # Send the knock!
                    
                    print(f"[*] Active Mode: Sending {filename} to client...")
                    rdt.gbn_send_file(filepath, active_socket, client_data_addr)
                    active_socket.close()
                
                client_conn.sendall(b"226 Transfer complete.\r\n")
            
            elif command.upper().startswith("STOR"):
                parts = command.split(" ", 1)
                if len(parts) < 2:
                    client_conn.sendall(b"501 Syntax error in parameters.\r\n")
                    continue
                
                filename = parts[1]
                filepath = os.path.join(current_dir, "uploaded_" + filename)
                
                if not is_pasv and not client_data_addr:
                    client_conn.sendall(b"425 Use PASV or PORT first.\r\n")
                    continue
                
                client_conn.sendall(b"150 Ready to receive file.\r\n")
                print(f"[*] Receiving {filename} from client...")
                
                if is_pasv:
                    rdt.gbn_receive_file(filepath, data_socket)
                    data_socket.close()
                    data_socket = None
                else:
                    # Active Mode STOR: Server binds a port and knocks the client to tell it where to send
                    active_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                    active_socket.bind((HOST, 0))
                    active_socket.sendto(b"KNOCK", client_data_addr)
                    
                    rdt.gbn_receive_file(filepath, active_socket)
                    active_socket.close()
                
                client_conn.sendall(b"226 Transfer complete.\r\n")
                
            elif command.upper().startswith("MKD"):
                parts = command.split(" ", 1)
                if len(parts) > 1:
                    target = os.path.join(current_dir, parts[1])
                    try:
                        os.mkdir(target)
                        client_conn.sendall(f"257 \"{parts[1]}\" directory created.\r\n".encode('utf-8'))
                    except FileExistsError:
                        client_conn.sendall(b"550 Directory already exists.\r\n")
                else:
                    client_conn.sendall(b"501 Syntax error in parameters.\r\n")

            elif command.upper().startswith("RMD"):
                parts = command.split(" ", 1)
                if len(parts) > 1:
                    target = os.path.join(current_dir, parts[1])
                    try:
                        os.rmdir(target)
                        client_conn.sendall(b"250 Directory removed.\r\n")
                    except FileNotFoundError:
                        client_conn.sendall(b"550 Directory not found.\r\n")
                    except OSError:
                        client_conn.sendall(b"550 Directory not empty or cannot be removed.\r\n")
                else:
                    client_conn.sendall(b"501 Syntax error in parameters.\r\n")

            elif command.upper().startswith("DELE"):
                parts = command.split(" ", 1)
                if len(parts) > 1:
                    target = os.path.join(current_dir, parts[1])
                    try:
                        os.remove(target)
                        client_conn.sendall(b"250 File deleted.\r\n")
                    except FileNotFoundError:
                        client_conn.sendall(b"550 File not found.\r\n")
                else:
                    client_conn.sendall(b"501 Syntax error in parameters.\r\n")

            elif command.upper().startswith("HASH"):
                parts = command.split(" ", 1)
                if len(parts) > 1:
                    target_file = os.path.join(current_dir, parts[1])
                    
                    if os.path.isfile(target_file):
                        hasher = hashlib.sha256()
                        with open(target_file, "rb") as f:
                            while True:
                                chunk = f.read(4096)
                                if not chunk:
                                    break
                                hasher.update(chunk)
                                
                        file_hash = hasher.hexdigest()
                        client_conn.sendall(f"213 {file_hash}\r\n".encode('utf-8'))
                    else:
                        client_conn.sendall(b"550 File not found.\r\n")
                else:
                    client_conn.sendall(b"501 Syntax error in parameters.\r\n")

            elif command.upper() == "QUIT":
                client_conn.sendall(b"221 Goodbye.\r\n")
                break
                
            else:
                client_conn.sendall(b"502 Command not implemented.\r\n")
                
    except Exception as e:
        print(f"[*] Connection error with {client_addr}: {e}")
        
    finally:
        # Guarantee socket cleanup when thread terminates
        client_conn.close()
        if data_socket:
            data_socket.close()
        print(f"[*] Connection with {client_addr} closed.")


# --- MAIN SERVER DISPATCHER ---
if __name__ == "__main__":
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    
    server_socket.bind((HOST, PORT))
    server_socket.listen(5) 
    
    print(f"[*] FTP Server listening on {HOST}:{PORT}...")
    
    try:
        while True:
            # Main thread waits here for incoming TCP connections
            client_conn, client_addr = server_socket.accept()
            
            # Spin up a dedicated thread for the new client
            client_thread = threading.Thread(target=handle_client, args=(client_conn, client_addr))
            # Daemon threads will automatically die if the main server is shut down
            client_thread.daemon = True 
            client_thread.start()
            
    except KeyboardInterrupt:
        print("\n[*] Server shutting down manually.")
        server_socket.close()