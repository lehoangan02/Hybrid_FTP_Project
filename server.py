import socket

# 1. Setup network constants
HOST = '127.0.0.1'  # Localhost (your own machine)
PORT = 2121         # Standard FTP control port is 21, but 2121 is safer for local testing

# 2. Create the TCP socket
# AF_INET = IPv4, SOCK_STREAM = TCP (reliable, connection-oriented)
server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

# Force the OS to release the port immediately when the server stops
server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

# 3. Bind the socket to the port and start listening
server_socket.bind((HOST, PORT))
server_socket.listen(1) # Listen for 1 connection at a time

print(f"[*] Server listening on {HOST}:{PORT}...")

# 4. Wait for a client to connect (this blocks until someone connects)
client_conn, client_addr = server_socket.accept()
print(f"[*] Client connected from {client_addr}")

# 5. Send the standard FTP welcome message
# FTP requires standard 3-digit reply codes followed by text.
welcome_msg = "220 Service ready for new user.\r\n"
client_conn.sendall(welcome_msg.encode('utf-8'))

# Clean up (for now)
client_conn.close()
server_socket.close()
print("[*] Server shut down.")