import socket

HOST = '127.0.0.1'
PORT = 2121

server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
# FIX: Allows restarting server immediately without 'Address already in use' error
server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

server_socket.bind((HOST, PORT))
server_socket.listen(5) # Allow a queue of up to 5 pending connections

print(f"[*] FTP Server listening on {HOST}:{PORT}...")

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