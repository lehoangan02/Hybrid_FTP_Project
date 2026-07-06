import socket

HOST = '127.0.0.1'
PORT = 2121

# 1. Create the TCP socket
client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

# 2. Connect to the server
print(f"[*] Connecting to {HOST}:{PORT}...")
client_socket.connect((HOST, PORT))

# 3. Receive the server's welcome message
response = client_socket.recv(1024) # Read up to 1024 bytes
print(f"Server says: {response.decode('utf-8')}")

# Clean up
client_socket.close()