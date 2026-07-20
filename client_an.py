# -*- coding: utf-8 -*-
"""
================================================================================
 HYBRID FTP CLIENT
 - Control Channel : TCP  (gui lenh, nhan phan hoi FTP-style tu server)
 - Data Channel    : UDP  (truyen file that su, dung tang RDT tu cai dat)

 Client dung CHUNG mot bo ham RDT (create_packet / verify_packet / rdt_send /
 rdt_recv) voi server.py, vi trong mo hinh nay ca hai phia deu co the la
 "nguoi gui" (STOR: client gui) hoac "nguoi nhan" (RETR: client nhan).

 Ho tro:
   - PASV (Passive): client goi "PASV", server mo cong UDP ngau nhien va tra
     ve (IP, port); client gui 1 goi "PING" de server hoc dia chi client.
   - PORT (Active): client tu mo 1 socket UDP, goi lenh "PORT" (khong can go
     tay IP/port) -> client tu dong sinh cau lenh PORT h1,h2,h3,h4,p1,p2 gui
     server; server se chu dong gui goi "READY" truoc, client nhan goi do de
     hoc dia chi UDP thuc su cua server.
   - RETR (download) va STOR (upload) deu dung chung 1 tang GBN + Congestion
     Control (AIMD) + kiem tra toan ven du lieu bang SHA256 (so sanh hash cuc
     bo voi hash server tra ve trong cau tra loi "226").
================================================================================
"""

import socket
import struct
import zlib
import re
import os
import select
import hashlib

HOST = '127.0.0.1'
PORT = 2121

# --- UDP CUSTOM HEADER (giong het server.py) ---
HEADER_FORMAT = '!I I B H I'
HEADER_SIZE = struct.calcsize(HEADER_FORMAT)
FLAG_DATA = 0x01
FLAG_ACK = 0x02
FLAG_FIN = 0x04

CHUNK_SIZE = 1024
TIMEOUT = 2.0
INITIAL_CWND = 1
SSTHRESH_INIT = 16
MAX_CWND = 32


# ============================================================
#  HAM DUNG CHUNG CHO TANG RDT (giong logic ben server.py)
# ============================================================
def create_packet(seq, ack, flags, data=b""):
    length = len(data)
    temp_header = struct.pack(HEADER_FORMAT, seq, ack, flags, length, 0)
    checksum = zlib.crc32(temp_header + data) & 0xffffffff
    header = struct.pack(HEADER_FORMAT, seq, ack, flags, length, checksum)
    return header + data


def verify_packet(packet):
    header = packet[:HEADER_SIZE]
    data = packet[HEADER_SIZE:]
    seq, ack, flags, length, recv_checksum = struct.unpack(HEADER_FORMAT, header)
    temp_header = struct.pack(HEADER_FORMAT, seq, ack, flags, length, 0)
    calc_checksum = zlib.crc32(temp_header + data) & 0xffffffff
    return calc_checksum == recv_checksum, seq, ack, flags, data


def rdt_send(udp_socket, remote_addr, filepath, tag="[UDP SEND]"):
    """Go-Back-N + AIMD Congestion Control. Tra ve (tong so goi, sha256_hex)."""
    with open(filepath, 'rb') as f:
        packets_data = []
        while True:
            chunk = f.read(CHUNK_SIZE)
            if not chunk:
                break
            packets_data.append(chunk)

    total_packets = len(packets_data)
    file_hash = hashlib.sha256(b"".join(packets_data)).hexdigest()
    print(f"{tag} Tong so goi tin can truyen: {total_packets}")

    base = 0
    next_seq_num = 0
    cwnd = INITIAL_CWND
    ssthresh = SSTHRESH_INIT

    while base < total_packets:
        window = min(int(cwnd), MAX_CWND)
        while next_seq_num < base + window and next_seq_num < total_packets:
            packet = create_packet(next_seq_num, 0, FLAG_DATA, packets_data[next_seq_num])
            udp_socket.sendto(packet, remote_addr)
            next_seq_num += 1

        ready = select.select([udp_socket], [], [], TIMEOUT)
        if ready[0]:
            ack_packet, _ = udp_socket.recvfrom(2048)
            ok, _, recv_ack, recv_flags, _ = verify_packet(ack_packet)
            if ok and recv_flags == FLAG_ACK and recv_ack >= base:
                base = recv_ack + 1
                if cwnd < ssthresh:
                    cwnd *= 2
                else:
                    cwnd += 1
                cwnd = min(cwnd, MAX_CWND)
        else:
            ssthresh = max(cwnd // 2, 2)
            cwnd = INITIAL_CWND
            next_seq_num = base
            print(f"{tag} Timeout! cwnd->{cwnd} ssthresh->{ssthresh}, resend tu base={base}")

    fin_packet = create_packet(total_packets, 0, FLAG_FIN)
    udp_socket.sendto(fin_packet, remote_addr)
    return total_packets, file_hash


def rdt_recv(udp_socket, save_path, tag="[UDP RECV]"):
    """Go-Back-N Receiver. Tra ve sha256_hex cua du lieu da nhan."""
    expected_seq = 0
    hasher = hashlib.sha256()

    with open(save_path, 'wb') as f:
        while True:
            packet, addr = udp_socket.recvfrom(2048)
            ok, seq, ack, flags, data = verify_packet(packet)

            if not ok:
                print(f"{tag} Loi checksum! Huy goi seq={seq}")
                continue

            if flags == FLAG_FIN:
                print(f"{tag} Nhan tin hieu FIN, hoan tat.")
                break

            if seq == expected_seq:
                f.write(data)
                hasher.update(data)
                ack_packet = create_packet(0, expected_seq, FLAG_ACK)
                udp_socket.sendto(ack_packet, addr)
                expected_seq += 1
            else:
                if expected_seq > 0:
                    ack_packet = create_packet(0, expected_seq - 1, FLAG_ACK)
                    udp_socket.sendto(ack_packet, addr)

    return hasher.hexdigest()


def local_file_hash(path):
    h = hashlib.sha256()
    with open(path, 'rb') as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


def print_hash_check(local_hash, server_reply):
    """So sanh hash cuc bo voi hash SHA256 server tra ve trong reply '226 ...'."""
    m = re.search(r'SHA256=([0-9a-fA-F]{64})', server_reply)
    if not m:
        return
    server_hash = m.group(1)
    if server_hash.lower() == local_hash.lower():
        print(f"[HASH CHECK] OK - Du lieu toan ven (SHA256={local_hash[:16]}...)")
    else:
        print(f"[HASH CHECK] LOI - SAI LECH DU LIEU! local={local_hash[:16]}... server={server_hash[:16]}...")


# ============================================================
#  MAIN: TCP CLIENT (dieu khien control channel + kich hoat data channel)
# ============================================================
def main():
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    print(f"[*] Connecting to {HOST}:{PORT}...")
    client_socket.connect((HOST, PORT))

    response = client_socket.recv(1024).decode('utf-8').strip()
    print(f"Server: {response}")

    udp_socket = None
    mode = None                  # "PASV" hoac "PORT"
    remote_data_addr = None      # dia chi UDP cua doi tac (server)

    while True:
        user_input = input("FTP> ").strip()
        if not user_input:
            continue

        cmd = user_input.split(' ')[0].upper()

        # --- PORT: client tu dong sinh lenh, khong can nguoi dung go IP/port ---
        if cmd == "PORT" and len(user_input.split(' ')) == 1:
            if udp_socket:
                udp_socket.close()
            udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            udp_socket.bind((HOST, 0))
            own_port = udp_socket.getsockname()[1]
            p1, p2 = own_port // 256, own_port % 256
            port_cmd = f"PORT 127,0,0,1,{p1},{p2}"

            client_socket.sendall((port_cmd + "\r\n").encode('utf-8'))
            server_reply = client_socket.recv(1024).decode('utf-8').strip()
            print(f"Server: {server_reply}")

            mode = "PORT"
            remote_data_addr = None   # se hoc duoc khi nhan goi 'READY' tu server
            continue

        # --- Cac lenh con lai: gui thang qua TCP control channel ---
        client_socket.sendall((user_input + "\r\n").encode('utf-8'))
        server_reply = client_socket.recv(4096).decode('utf-8').strip()
        print(f"Server: {server_reply}")

        if cmd == "PASV" and "227" in server_reply:
            match = re.search(r'\((\d+),(\d+),(\d+),(\d+),(\d+),(\d+)\)', server_reply)
            if match:
                p1, p2 = int(match.group(5)), int(match.group(6))
                data_port = (p1 * 256) + p2
                remote_data_addr = (HOST, data_port)
                mode = "PASV"
                if udp_socket:
                    udp_socket.close()
                udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                print(f"[*] (PASV) Server san sang tren {remote_data_addr}")

        elif cmd == "RETR" and "150" in server_reply:
            filename = user_input.split(' ')[1]
            save_filename = "downloaded_" + filename

            if mode == "PASV":
                udp_socket.sendto(b"PING", remote_data_addr)
            else:  # PORT (active): server chu dong gui 'READY' truoc
                _, remote_data_addr = udp_socket.recvfrom(1024)

            print(f"[*] Bat dau tai file '{filename}' qua UDP GBN...")
            local_hash = rdt_recv(udp_socket, save_filename)

            final_reply = client_socket.recv(4096).decode('utf-8').strip()
            print(f"Server: {final_reply}")
            print_hash_check(local_hash, final_reply)

            udp_socket.close()
            udp_socket = None
            mode = None

        elif cmd == "STOR" and "150" in server_reply:
            filename = user_input.split(' ')[1]

            if not os.path.isfile(filename):
                print(f"[Loi] Khong tim thay file local: {filename}")
            else:
                if mode == "PASV":
                    udp_socket.sendto(b"PING", remote_data_addr)
                else:  # PORT (active): cho goi 'READY' tu server truoc khi gui
                    _, remote_data_addr = udp_socket.recvfrom(1024)

                print(f"[*] Bat dau upload file '{filename}' qua UDP GBN...")
                _, local_hash = rdt_send(udp_socket, remote_data_addr, filename)

                final_reply = client_socket.recv(4096).decode('utf-8').strip()
                print(f"Server: {final_reply}")
                print_hash_check(local_hash, final_reply)

            udp_socket.close()
            udp_socket = None
            mode = None

        if cmd == "QUIT":
            break

    client_socket.close()


if __name__ == "__main__":
    main()
