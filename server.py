import socket
import os
import rdt
import hashlib
import threading
import time
import uuid

HOST = '127.0.0.1'
PORT = 2121

def get_safe_path(base_dir, current_dir, target_path):
    """Safely resolve target_path ensuring it doesn't escape base_dir."""
    target_path = target_path.lstrip('/\\')
    resolved_path = os.path.abspath(os.path.join(current_dir, target_path))
    if resolved_path.startswith(os.path.abspath(base_dir)):
        return resolved_path
    return None

def handle_client(client_conn, client_addr):
    """Handles an isolated session for a single connected client."""
    print(f"\n[*] New client connected from {client_addr}")
    
    # Thread-local state: Each client tracks its own directory and data channel
    current_dir = os.getcwd()
    base_dir = current_dir
    data_socket = None

    is_pasv = False
    client_data_addr = None
    username = None
    is_authenticated = False
    rename_from_path = None
    
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
            
            command_type = command.split(" ", 1)[0].upper()
            if command_type not in ["USER", "PASS", "QUIT", "HELP"] and not is_authenticated:
                client_conn.sendall(b"530 Not logged in.\r\n")
                continue

            if command_type == "USER":
                parts = command.split(" ", 1)
                if len(parts) > 1:
                    username = parts[1]
                    client_conn.sendall(b"331 Username OK, need password.\r\n")
                else:
                    client_conn.sendall(b"501 Syntax error in parameters.\r\n")
                
            elif command_type == "PASS":
                parts = command.split(" ", 1)
                if len(parts) > 1 and username:
                    password = parts[1]
                    if username == "admin" and password == "password123":
                        is_authenticated = True
                        client_conn.sendall(b"230 User logged in successfully.\r\n")
                    else:
                        client_conn.sendall(b"530 Authentication failed.\r\n")
                else:
                    client_conn.sendall(b"501 Syntax error or USER command missing.\r\n")
            
            elif command.upper() == "PWD":
                reply = f"257 \"{current_dir}\" is current directory.\r\n"
                client_conn.sendall(reply.encode('utf-8'))
                
            elif command.upper().startswith("CWD"):
                parts = command.split(" ", 1)
                
                if len(parts) > 1:
                    target_dir = parts[1]
                    # FIX: Safely calculate new path without changing global server OS state
                    potential_dir = get_safe_path(base_dir, current_dir, target_dir)
                    
                    if potential_dir and os.path.isdir(potential_dir):
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
                
                is_pasv = True
                reply = f"227 Entering Passive Mode ({ip_parts[0]},{ip_parts[1]},{ip_parts[2]},{ip_parts[3]},{p1},{p2}).\r\n"
                client_conn.sendall(reply.encode('utf-8'))
                print(f"[*] Prepared Passive UDP Data Channel on port {data_port}")
            
            elif command.upper().startswith("LIST") or command.upper().startswith("NLST"):
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
                filepath = get_safe_path(base_dir, current_dir, filename)
                
                if not filepath or not os.path.isfile(filepath):
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
                filepath = get_safe_path(base_dir, current_dir, "uploaded_" + filename)
                if not filepath:
                    client_conn.sendall(b"550 Access denied.\r\n")
                    continue
                
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

            elif command.upper().startswith("STOU"):
                if not is_pasv and not client_data_addr:
                    client_conn.sendall(b"425 Use PASV or PORT first.\r\n")
                    continue
                
                parts = command.split(" ", 1)
                prefix = "uploaded_"
                if len(parts) > 1:
                    prefix += parts[1] + "_"
                
                unique_name = prefix + uuid.uuid4().hex[:8]
                filepath = get_safe_path(base_dir, current_dir, unique_name)
                
                if not filepath:
                    client_conn.sendall(b"550 Access denied.\r\n")
                    continue
                
                client_conn.sendall(f"150 FILE: {unique_name} Ready to receive file.\r\n".encode('utf-8'))
                print(f"[*] Receiving unique file {unique_name} from client...")
                
                if is_pasv:
                    rdt.gbn_receive_file(filepath, data_socket)
                    data_socket.close()
                    data_socket = None
                else:
                    active_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                    active_socket.bind((HOST, 0))
                    active_socket.sendto(b"KNOCK", client_data_addr)
                    rdt.gbn_receive_file(filepath, active_socket)
                    active_socket.close()
                
                client_conn.sendall(b"226 Transfer complete.\r\n")
                
            elif command.upper().startswith("MKD"):
                parts = command.split(" ", 1)
                if len(parts) > 1:
                    target = get_safe_path(base_dir, current_dir, parts[1])
                    if not target:
                        client_conn.sendall(b"550 Access denied.\r\n")
                        continue
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
                    target = get_safe_path(base_dir, current_dir, parts[1])
                    if not target:
                        client_conn.sendall(b"550 Access denied.\r\n")
                        continue
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
                    target = get_safe_path(base_dir, current_dir, parts[1])
                    if not target:
                        client_conn.sendall(b"550 Access denied.\r\n")
                        continue
                    try:
                        os.remove(target)
                        client_conn.sendall(b"250 File deleted.\r\n")
                    except FileNotFoundError:
                        client_conn.sendall(b"550 File not found.\r\n")
                else:
                    client_conn.sendall(b"501 Syntax error in parameters.\r\n")

            elif command.upper().startswith("RNFR"):
                parts = command.split(" ", 1)
                if len(parts) > 1:
                    target = get_safe_path(base_dir, current_dir, parts[1])
                    if target and os.path.exists(target):
                        rename_from_path = target
                        client_conn.sendall(b"350 Requested file action pending further information.\r\n")
                    else:
                        client_conn.sendall(b"550 File not found.\r\n")
                else:
                    client_conn.sendall(b"501 Syntax error in parameters.\r\n")
            
            elif command.upper().startswith("RNTO"):
                parts = command.split(" ", 1)
                if len(parts) > 1:
                    if rename_from_path:
                        target = get_safe_path(base_dir, current_dir, parts[1])
                        if target:
                            try:
                                os.rename(rename_from_path, target)
                                client_conn.sendall(b"250 Requested file action OK, file renamed.\r\n")
                            except OSError:
                                client_conn.sendall(b"550 Rename failed.\r\n")
                        else:
                            client_conn.sendall(b"550 Access denied for target path.\r\n")
                        rename_from_path = None
                    else:
                        client_conn.sendall(b"503 Bad sequence of commands.\r\n")
                else:
                    client_conn.sendall(b"501 Syntax error in parameters.\r\n")
                    
            elif command.upper() == "ABOR":
                if data_socket:
                    data_socket.close()
                    data_socket = None
                is_pasv = False
                client_data_addr = None
                client_conn.sendall(b"226 Abort successful.\r\n")

            elif command.upper().startswith("HASH"):
                parts = command.split(" ", 1)
                if len(parts) > 1:
                    target_file = get_safe_path(base_dir, current_dir, parts[1])
                    
                    if target_file and os.path.isfile(target_file):
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

            elif command.upper() == "CDUP":
                potential_dir = get_safe_path(base_dir, current_dir, "..")
                if potential_dir and os.path.isdir(potential_dir):
                    current_dir = potential_dir
                    client_conn.sendall(b"250 Directory changed to parent.\r\n")
                else:
                    client_conn.sendall(b"550 Cannot change to parent directory.\r\n")

            elif command.upper().startswith("STAT"):
                parts = command.split(" ", 1)
                if len(parts) > 1:
                    target = get_safe_path(base_dir, current_dir, parts[1])
                    if target and os.path.exists(target):
                        stats = os.stat(target)
                        reply = f"213-Status of {parts[1]}:\r\n Size: {stats.st_size} bytes\r\n213 End of status.\r\n"
                        client_conn.sendall(reply.encode('utf-8'))
                    else:
                        client_conn.sendall(b"550 File not found.\r\n")
                else:
                    reply = "211-Server status:\r\n Hybrid FTP Server v1.0\r\n Mode: Active/Passive\r\n211 End of status.\r\n"
                    client_conn.sendall(reply.encode('utf-8'))

            elif command.upper().startswith("SIZE"):
                parts = command.split(" ", 1)
                if len(parts) > 1:
                    target_file = get_safe_path(base_dir, current_dir, parts[1])
                    if target_file and os.path.isfile(target_file):
                        size = os.path.getsize(target_file)
                        client_conn.sendall(f"213 {size}\r\n".encode('utf-8'))
                    else:
                        client_conn.sendall(b"550 File not found.\r\n")
                else:
                    client_conn.sendall(b"501 Syntax error in parameters.\r\n")

            elif command.upper().startswith("MDTM"):
                parts = command.split(" ", 1)
                if len(parts) > 1:
                    target_file = get_safe_path(base_dir, current_dir, parts[1])
                    if target_file and os.path.isfile(target_file):
                        mtime = os.path.getmtime(target_file)
                        time_str = time.strftime('%Y%m%d%H%M%S', time.localtime(mtime))
                        client_conn.sendall(f"213 {time_str}\r\n".encode('utf-8'))
                    else:
                        client_conn.sendall(b"550 File not found.\r\n")
                else:
                    client_conn.sendall(b"501 Syntax error in parameters.\r\n")

            elif command.upper() == "NOOP":
                client_conn.sendall(b"200 NOOP OK.\r\n")
                
            elif command.upper().startswith("TYPE"):
                parts = command.split(" ", 1)
                type_val = parts[1] if len(parts) > 1 else "I"
                client_conn.sendall(f"200 Type set to {type_val}.\r\n".encode('utf-8'))
                
            elif command.upper().startswith("MODE"):
                parts = command.split(" ", 1)
                mode_val = parts[1] if len(parts) > 1 else "S"
                client_conn.sendall(f"200 Mode set to {mode_val}.\r\n".encode('utf-8'))
                
            elif command.upper().startswith("HELP"):
                help_msg = (
                    "214-The following commands are recognized:\r\n"
                    " USER PASS PWD CWD CDUP LIST PASV PORT RETR STOR MKD RMD DELE SIZE MDTM HASH NOOP TYPE MODE HELP QUIT\r\n"
                    "214 Help OK.\r\n"
                )
                client_conn.sendall(help_msg.encode('utf-8'))

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