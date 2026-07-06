import socket

HOST = '127.0.0.1'
PORT = 2121

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
    client_socket.sendall((user_input + "\r\n").encode('utf-8'))
    
    # Wait for server response
    server_reply = client_socket.recv(1024)
    print(server_reply.decode('utf-8').strip())
    
    # If we sent QUIT, break out of our local loop 
    if user_input.upper() == "QUIT":
        break

client_socket.close()
print("[*] Disconnected.")