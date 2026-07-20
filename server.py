import socket
import os

HOST = '127.0.0.1'
PORT = 2121

server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
# FIX: Allows restarting server immediately without 'Address already in use' error
server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

server_socket.bind((HOST, PORT))
server_socket.listen(5) # Allow a queue of up to 5 pending connections

print(f"[*] FTP Server listening on {HOST}:{PORT}...")

# Track where the server is currently looking on the hard drive
current_dir = os.getcwd()

try:
    while True:
        # Wait for a client connection
        client_conn, client_addr = server_socket.accept()
        print(f"\n[*] New client connected from {client_addr}")
        
        # Send standard FTP welcome code [cite: 332, 396]
        client_conn.sendall(b"220 Service ready for new user.\r\n")
        
        # Inner loop to handle multiple commands from this specific client
        while True:
            data = client_conn.recv(1024)
            if not data:
                break # Client disconnected abruptly
                
            command = data.decode('utf-8').strip()
            print(f"[Client says]: {command}")
            
            # Simple Project Rule Command Parsing 
            if command.upper().startswith("USER"):
                # E.g., USER admin -> reply with 331 [cite: 389, 396]
                client_conn.sendall(b"331 Username OK, need password.\r\n")
                
            elif command.upper().startswith("PASS"):
                # E.g., PASS 1234 -> reply with 230 [cite: 389, 396]
                client_conn.sendall(b"230 User logged in successfully.\r\n")
            
            elif command.upper() == "PWD":
                # Print Working Directory: Tells the client their current path 
                reply = f"257 \"{current_dir}\" is current directory.\r\n"
                client_conn.sendall(reply.encode('utf-8'))
                
            elif command.upper().startswith("CWD"):
                # Change Working Directory: Moves to a new folder 
                # Split the command (e.g., "CWD Documents" -> ["CWD", "Documents"])
                parts = command.split(" ", 1)
                
                if len(parts) > 1:
                    target_dir = parts[1]
                    try:
                        os.chdir(target_dir)       # Ask the OS to change folders
                        current_dir = os.getcwd()  # Update our tracker
                        client_conn.sendall(b"250 Requested file action OK.\r\n") # [cite: 396]
                    except FileNotFoundError:
                        # The folder doesn't exist
                        client_conn.sendall(b"550 File unavailable or not found.\r\n") # [cite: 396]
                else:
                    # The user typed "CWD" but forgot to name a folder
                    client_conn.sendall(b"501 Syntax error in parameters.\r\n") # [cite: 396]
            
            elif command.upper() == "PASV":
                # 1. Create a UDP socket for the Data Channel
                # Note: AF_INET (IPv4), SOCK_DGRAM (UDP)
                data_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                
                # 2. Bind to the host, but use port 0. 
                # Port 0 tells the OS: "Pick any random available port for me."
                data_socket.bind((HOST, 0))
                
                # 3. Find out which port the OS actually gave us
                _, data_port = data_socket.getsockname()
                
                # 4. FTP requires a very specific format for the IP and Port: (h1,h2,h3,h4,p1,p2)
                # Split the IP address by periods
                ip_parts = HOST.split('.') 
                
                # Calculate the port numbers (FTP quirk: Port = p1 * 256 + p2)
                p1 = data_port // 256
                p2 = data_port % 256
                
                # 5. Send the formatted 227 response
                reply = f"227 Entering Passive Mode ({ip_parts[0]},{ip_parts[1]},{ip_parts[2]},{ip_parts[3]},{p1},{p2}).\r\n"
                client_conn.sendall(reply.encode('utf-8'))
                
                # For right now, we will just close the data socket immediately after creating it.
                # In the next step, we will use it to actually send file data!
                data_socket.close()
                print(f"[*] Prepared Passive UDP Data Channel on port {data_port}")

            elif command.upper() == "QUIT":
                # Gracefully close session 
                client_conn.sendall(b"221 Goodbye.\r\n")
                break
                
            else:
                # Command not supported yet 
                client_conn.sendall(b"502 Command not implemented.\r\n")
                
        client_conn.close()
        print(f"[*] Connection with {client_addr} closed.")

except KeyboardInterrupt:
    print("\n[*] Server shutting down manually.")
    server_socket.close()