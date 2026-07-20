# -*- coding: utf-8 -*-
"""
================================================================================
 HYBRID FTP SERVER
 - Control Channel : TCP  (commands + responses, like RFC 959)
 - Data Channel    : UDP  (file transfer, implementing Reliable Data
                     Transfer from scratch, no external libraries)

 The custom reliable layer (RDT) consists of:
   1) Custom UDP Header: Sequence(4B) + Ack(4B) + Flags(1B) + Length(2B) +
      Checksum(4B) = 15 bytes, using CRC32 (zlib) for bit error detection.
   2) Go-Back-N (GBN): sliding window sender, Cumulative ACK receiver,
      timeout -> retransmit entire window.
   3) Congestion Control AIMD (simulating TCP):
        - Slow Start: cwnd doubles every round until ssthresh
        - Congestion Avoidance: cwnd += 1 every round when cwnd >= ssthresh
        - Timeout -> Multiplicative Decrease: ssthresh = cwnd/2, cwnd = 1
   4) Data Integrity: calculate SHA256 of file, included in "226" response
      for client to verify (end-to-end hash verification).

 Additionally:
   - Supports both PASV (Passive) and PORT (Active) modes.
   - Each TCP connection from a client is handled on a separate thread
     (threading.Thread) -> supports multiple concurrent clients, independent sessions.
   - Some directory navigation commands (PWD/CWD/CDUP/MKD/RMD/LIST/DELE/SIZE/MDTM)
     are added to achieve Advanced level (Directory Navigation).
================================================================================
"""

import socket
import struct
import zlib
import os
import select
import threading
import hashlib
import time

# ============================================================
#  GENERAL CONFIGURATION
# ============================================================
HOST = '127.0.0.1'
PORT = 2121

# All files for RETR/STOR are stored in this directory (prevents Path Traversal)
BASE_DIR = os.path.abspath(os.path.join(os.getcwd(), "server_storage"))
os.makedirs(BASE_DIR, exist_ok=True)

# --- UDP CUSTOM HEADER ---
# Format: Sequence (4B) | Ack (4B) | Flags (1B) | Length (2B) | Checksum (4B)
HEADER_FORMAT = '!I I B H I'
HEADER_SIZE = struct.calcsize(HEADER_FORMAT)
# Current header is 15 bytes: 4 (seq) + 4 (ack) + 1 (flags) + 2 (length) + 4 (checksum) = 15 bytes 
# Header size can be increased/decreased to 20 bytes if desired, but 15 bytes is currently enough for necessary fields.
FLAG_DATA = 0x01
FLAG_ACK = 0x02
FLAG_FIN = 0x04

CHUNK_SIZE = 1024      # payload size per packet (bytes)
TIMEOUT = 2.0          # timeout waiting for ACK (seconds)

# --- Congestion Control Parameters (AIMD) ---
INITIAL_CWND = 1       # initial cwnd (Slow Start starts at 1 MSS)
SSTHRESH_INIT = 16     # threshold to switch from Slow Start -> Congestion Avoidance
MAX_CWND = 32          # upper bound to prevent overwhelming receiver (acts as rwnd)


# ============================================================
#  COMMON FUNCTIONS FOR RDT LAYER (Reliable Data Transfer)
# ============================================================
def create_packet(seq, ack, flags, data=b""):
    """Package application layer data with Custom UDP Header + CRC32 Checksum."""
    length = len(data)
    temp_header = struct.pack(HEADER_FORMAT, seq, ack, flags, length, 0) # Temporary packaging with checksum=0 to calculate checksum
    checksum = zlib.crc32(temp_header + data) & 0xffffffff # This calculates CRC32 and ensures checksum is a non-negative 32-bit integer
    header = struct.pack(HEADER_FORMAT, seq, ack, flags, length, checksum) 
    return header + data


def verify_packet(packet):
    """Extract header/payload and verify checksum. Returns (ok, seq, ack, flags, data)."""
    header = packet[:HEADER_SIZE] # Get header part from packet (first 15 bytes)
    data = packet[HEADER_SIZE:] # Get data part from packet (after first 15 bytes)
    seq, ack, flags, length, recv_checksum = struct.unpack(HEADER_FORMAT, header)
    temp_header = struct.pack(HEADER_FORMAT, seq, ack, flags, length, 0) 
    calc_checksum = zlib.crc32(temp_header + data) & 0xffffffff 
    return calc_checksum == recv_checksum, seq, ack, flags, data


def rdt_send(udp_socket, remote_addr, filepath, tag="[UDP SEND]"):
    """
    Send a file over UDP using Go-Back-N + Congestion Control AIMD.
    Returns (total packets, sha256_hex) for data integrity verification.
    """
    with open(filepath, 'rb') as f:
        packets_data = []
        while True:
            chunk = f.read(CHUNK_SIZE) # Read data chunks from file with CHUNK_SIZE (1024 bytes)
            if not chunk:
                break
            packets_data.append(chunk)

    total_packets = len(packets_data)
    file_hash = hashlib.sha256(b"".join(packets_data)).hexdigest()
    print(f"{tag} Total packets to transmit: {total_packets} | SHA256={file_hash[:16]}...")

    base = 0
    next_seq_num = 0
    cwnd = INITIAL_CWND        # current effective window size
    ssthresh = SSTHRESH_INIT

    while base < total_packets:
        window = min(int(cwnd), MAX_CWND)

        # Phase 1: push maximum packets within window scope (limited by cwnd)
        while next_seq_num < base + window and next_seq_num < total_packets:
            packet = create_packet(next_seq_num, 0, FLAG_DATA, packets_data[next_seq_num])
            udp_socket.sendto(packet, remote_addr)
            next_seq_num += 1

        # Phase 2: wait for ACK up to TIMEOUT seconds (non-blocking using select)
        ready = select.select([udp_socket], [], [], TIMEOUT)

        if ready[0]: # data arrived from UDP socket (ACK from client)
            ack_packet, _ = udp_socket.recvfrom(2048)
            ok, _, recv_ack, recv_flags, _ = verify_packet(ack_packet)

            if ok and recv_flags == FLAG_ACK and recv_ack >= base:
                base = recv_ack + 1
                # --- Congestion Control: received valid ACK -> increase cwnd ---
                if cwnd < ssthresh:
                    cwnd *= 2          # Slow Start: increase exponentially
                else:
                    cwnd += 1          # Congestion Avoidance: increase linearly
                cwnd = min(cwnd, MAX_CWND)
        else:
            # Phase 3: Timeout -> Go-Back-N (resend from base)
            # + Multiplicative Decrease for Congestion Control
            ssthresh = max(cwnd // 2, 2)
            cwnd = INITIAL_CWND
            next_seq_num = base
            print(f"{tag} Timeout! cwnd->{cwnd} ssthresh->{ssthresh}, resend from base={base}")

    # Phase 4: signal end with FIN packet
    fin_packet = create_packet(total_packets, 0, FLAG_FIN)
    udp_socket.sendto(fin_packet, remote_addr)
    return total_packets, file_hash


def rdt_recv(udp_socket, save_path, tag="[UDP RECV]"):
    """
    Receive a file over UDP using Go-Back-N Receiver: only accept in-order packets,
    drop checksum error or out-of-order packets, return Cumulative ACK.
    Returns sha256_hex of written data (used for matching with sender).
    """
    expected_seq = 0
    hasher = hashlib.sha256() 

    with open(save_path, 'wb') as f:
        while True:
            packet, addr = udp_socket.recvfrom(2048) #
            ok, seq, ack, flags, data = verify_packet(packet)

            if not ok: # If checksum is incorrect -> drop, resend latest ACK (expected_seq-1)
                print(f"{tag} Checksum error! Drop packet seq={seq}")
                continue

            if flags == FLAG_FIN: # If FIN packet received -> end file transfer
                print(f"{tag} FIN signal received, file transfer complete.")
                break

            if seq == expected_seq: # If packet in order -> write to file, update hash, send ACK
                f.write(data)
                hasher.update(data)
                ack_packet = create_packet(0, expected_seq, FLAG_ACK)
                udp_socket.sendto(ack_packet, addr)
                expected_seq += 1
            else:
                # Out-of-order / duplicate packet -> drop, resend latest ACK
                if expected_seq > 0:
                    ack_packet = create_packet(0, expected_seq - 1, FLAG_ACK)
                    udp_socket.sendto(ack_packet, addr)

    return hasher.hexdigest() # Return SHA256 of received data for client/server to verify data integrity


def file_sha256(path):
    h = hashlib.sha256()
    with open(path, 'rb') as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


# ============================================================
#  DIRECTORY OPERATION UTILITIES (safe, confined to BASE_DIR)
# Join and normalize paths, check if it escapes BASE_DIR (Path Traversal)
# ============================================================
def safe_join(cwd_abs, rel_path):
    """Join path and ensure result does not escape BASE_DIR."""
    if not rel_path:
        rel_path = "."
    target = os.path.normpath(os.path.join(cwd_abs, rel_path))
    if not (target == BASE_DIR or target.startswith(BASE_DIR + os.sep)):
        return None
    return target


# ============================================================
#  PROCESS 1 CLIENT SESSION (runs on a separate thread -> supports multi-client)
# ============================================================
def handle_client(client_conn, client_addr):
    print(f"\n[*] New client connected from {client_addr}")
    client_conn.sendall(b"220 Service ready for new user.\r\n")

    cwd_abs = BASE_DIR
    udp_socket = None
    mode = None                 # "PASV" or "PORT"
    remote_data_addr = None     # client's UDP address (known via PASV-ping or PORT-cmd)

    try:
        while True:
            data = client_conn.recv(1024)
            if not data:
                break
            command = data.decode('utf-8', errors='ignore').strip()
            if not command:
                continue
            print(f"[TCP {client_addr}] >> {command}")

            parts = command.split(' ')
            cmd = parts[0].upper()
            arg = parts[1] if len(parts) > 1 else ""

            # ---------------- Authentication ----------------
            if cmd == "USER":
                client_conn.sendall(b"331 Username OK, need password.\r\n")
            elif cmd == "PASS":
                client_conn.sendall(b"230 User logged in successfully.\r\n")
            elif cmd == "NOOP":
                client_conn.sendall(b"200 NOOP OK.\r\n")
            elif cmd == "TYPE":
                client_conn.sendall(f"200 Type set to {arg or 'I'}.\r\n".encode())
            elif cmd == "MODE":
                client_conn.sendall(f"200 Mode set to {arg or 'S'}.\r\n".encode())
            elif cmd == "HELP":
                client_conn.sendall(b"214 Commands: USER PASS PWD CWD CDUP MKD RMD "
                                     b"LIST DELE SIZE MDTM HASH PASV PORT RETR STOR QUIT\r\n")

            # ---------------- Directory navigation (Advanced level) ----------------
            elif cmd == "PWD":
                rel = os.path.relpath(cwd_abs, BASE_DIR).replace("\\", "/")
                rel = "/" if rel == "." else "/" + rel
                client_conn.sendall(f'257 "{rel}" is current directory.\r\n'.encode())
            elif cmd == "CWD":
                new_dir = safe_join(cwd_abs, arg)
                if new_dir and os.path.isdir(new_dir):
                    cwd_abs = new_dir
                    client_conn.sendall(b"250 Directory changed.\r\n")
                else:
                    client_conn.sendall(b"550 Directory not found.\r\n")
            elif cmd == "CDUP":
                cwd_abs = safe_join(cwd_abs, "..") or BASE_DIR
                client_conn.sendall(b"250 Directory changed to parent.\r\n")
            elif cmd == "MKD":
                new_dir = safe_join(cwd_abs, arg)
                if new_dir:
                    try:
                        os.makedirs(new_dir, exist_ok=False)
                        client_conn.sendall(b"257 Directory created.\r\n")
                    except FileExistsError:
                        client_conn.sendall(b"550 Directory already exists.\r\n")
                else:
                    client_conn.sendall(b"550 Invalid path.\r\n")
            elif cmd == "RMD":
                target = safe_join(cwd_abs, arg)
                if target and os.path.isdir(target):
                    try:
                        os.rmdir(target)
                        client_conn.sendall(b"250 Directory removed.\r\n")
                    except OSError:
                        client_conn.sendall(b"550 Directory not empty or error.\r\n")
                else:
                    client_conn.sendall(b"550 Directory not found.\r\n")
            elif cmd in ("LIST", "NLST"):
                target = safe_join(cwd_abs, arg) if arg else cwd_abs
                if target and os.path.isdir(target):
                    entries = sorted(os.listdir(target))
                    if cmd == "LIST":
                        lines = []
                        for e in entries:
                            p = os.path.join(target, e)
                            size = os.path.getsize(p)
                            kind = "DIR " if os.path.isdir(p) else "FILE"
                            lines.append(f"{kind} {size:>10}  {e}")
                        body = "\r\n".join(lines) if lines else "(empty)"
                    else:
                        body = "\r\n".join(entries) if entries else "(empty)"
                    client_conn.sendall(
                        f"150 Here comes the directory listing.\r\n{body}\r\n226 Directory send OK.\r\n".encode())
                else:
                    client_conn.sendall(b"550 Directory not found.\r\n")
            elif cmd == "SIZE":
                target = safe_join(cwd_abs, arg)
                if target and os.path.isfile(target):
                    client_conn.sendall(f"213 {os.path.getsize(target)}\r\n".encode())
                else:
                    client_conn.sendall(b"550 File not found.\r\n")
            elif cmd == "MDTM":
                target = safe_join(cwd_abs, arg)
                if target and os.path.isfile(target):
                    ts = time.strftime('%Y%m%d%H%M%S', time.localtime(os.path.getmtime(target)))
                    client_conn.sendall(f"213 {ts}\r\n".encode())
                else:
                    client_conn.sendall(b"550 File not found.\r\n")
            elif cmd == "DELE":
                target = safe_join(cwd_abs, arg)
                if target and os.path.isfile(target):
                    os.remove(target)
                    client_conn.sendall(b"250 File deleted.\r\n")
                else:
                    client_conn.sendall(b"550 File not found.\r\n")
            elif cmd == "HASH":
                target = safe_join(cwd_abs, arg)
                if target and os.path.isfile(target):
                    client_conn.sendall(f"213 SHA256 {file_sha256(target)}\r\n".encode())
                else:
                    client_conn.sendall(b"550 File not found.\r\n")

            # ---------------- Data channel setup ----------------
            elif cmd == "PASV":
                if udp_socket:
                    udp_socket.close()
                udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                udp_socket.bind((HOST, 0))
                data_port = udp_socket.getsockname()[1]
                p1, p2 = data_port // 256, data_port % 256
                mode = "PASV"
                remote_data_addr = None
                client_conn.sendall(f"227 Entering Passive Mode (127,0,0,1,{p1},{p2}).\r\n".encode())
                print(f"[*] (PASV) Opened UDP data channel on port {data_port}")

            elif cmd == "PORT":
                # Syntax: PORT h1,h2,h3,h4,p1,p2  (Active Mode)
                try:
                    nums = list(map(int, arg.split(',')))
                    h1, h2, h3, h4, p1, p2 = nums
                    client_ip = f"{h1}.{h2}.{h3}.{h4}"
                    client_port = p1 * 256 + p2
                    if udp_socket:
                        udp_socket.close()
                    udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                    udp_socket.bind((HOST, 0))   # server automatically selects source port (simulating port 20)
                    mode = "PORT"
                    remote_data_addr = (client_ip, client_port)
                    client_conn.sendall(b"200 PORT command successful.\r\n")
                    print(f"[*] (PORT/Active) Server will actively connect to {remote_data_addr}")
                except Exception:
                    client_conn.sendall(b"501 Syntax error in PORT.\r\n")

            # ---------------- File transfer ----------------
            elif cmd == "RETR":
                if not arg:
                    client_conn.sendall(b"501 Syntax error in parameters.\r\n")
                    continue
                target = safe_join(cwd_abs, arg)
                if not udp_socket:
                    client_conn.sendall(b"425 Use PASV or PORT first.\r\n")
                    continue
                if not target or not os.path.isfile(target):
                    client_conn.sendall(b"550 File unavailable.\r\n")
                    continue

                client_conn.sendall(b"150 File status okay, opening data connection.\r\n")

                if mode == "PASV":
                    print("[UDP] (PASV) Waiting for client UDP ping...")
                    _, remote_data_addr = udp_socket.recvfrom(1024)
                else:
                    # Active mode: server actively "handshakes" before sending data
                    udp_socket.sendto(b"READY", remote_data_addr)

                print(f"[*] Started UDP GBN sending file (server->client) to {remote_data_addr}...")
                total, file_hash = rdt_send(udp_socket, remote_data_addr, target, tag="[UDP GBN SEND]")

                client_conn.sendall(f"226 Transfer complete. packets={total} SHA256={file_hash}\r\n".encode())
                udp_socket.close()
                udp_socket = None
                mode = None

            elif cmd == "STOR":
                if not arg:
                    client_conn.sendall(b"501 Syntax error in parameters.\r\n")
                    continue
                target = safe_join(cwd_abs, arg)
                if not udp_socket or not target:
                    client_conn.sendall(b"425 Use PASV or PORT first.\r\n")
                    continue

                client_conn.sendall(b"150 File status okay, opening data connection.\r\n")

                if mode == "PASV":
                    print("[UDP] (PASV) Waiting for client UDP ping...")
                    _, remote_data_addr = udp_socket.recvfrom(1024)
                else:
                    udp_socket.sendto(b"READY", remote_data_addr)

                print(f"[*] Started UDP GBN receiving file (client->server) from {remote_data_addr}...")
                file_hash = rdt_recv(udp_socket, target, tag="[UDP GBN RECV]")

                client_conn.sendall(f"226 Transfer complete. SHA256={file_hash}\r\n".encode())
                udp_socket.close()
                udp_socket = None
                mode = None

            elif cmd == "QUIT":
                client_conn.sendall(b"221 Goodbye.\r\n")
                break
            else:
                client_conn.sendall(b"502 Command not implemented.\r\n")

    except (ConnectionResetError, BrokenPipeError):
        pass
    finally:
        client_conn.close()
        if udp_socket:
            udp_socket.close()
        print(f"[*] Connection {client_addr} closed.")


# ============================================================
#  MAIN: TCP SERVER (multi-threaded, each client has independent thread)
# ============================================================
def main():
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_socket.bind((HOST, PORT))
    server_socket.listen(5)

    print(f"[*] Hybrid FTP Server listening at {HOST}:{PORT}")
    print(f"[*] Storage directory: {BASE_DIR}")

    try:
        while True:
            client_conn, client_addr = server_socket.accept()
            t = threading.Thread(target=handle_client, args=(client_conn, client_addr), daemon=True)
            t.start()
    except KeyboardInterrupt:
        print("\n[*] Server shutting down...")
    finally:
        server_socket.close()


if __name__ == "__main__":
    main()
