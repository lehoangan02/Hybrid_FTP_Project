import struct
import zlib
import socket

# Custom Flags for our protocol
FLAG_DATA = 0x01
FLAG_ACK = 0x02
FLAG_FIN = 0x04

# 15-byte Header format: !IIIHB (4B Seq, 4B Ack, 4B Checksum, 2B Len, 1B Flag)[cite: 1]
HEADER_FORMAT = '!IIIHB'
HEADER_SIZE = 15
MAX_PAYLOAD = 4096 - HEADER_SIZE 

def make_packet(seq_num, ack_num, flags, data=b""):
    """Packs the 15-byte header and calculates the CRC32 checksum."""
    length = len(data)
    # Pack with a temporary 0 checksum to calculate the real checksum
    temp_header = struct.pack(HEADER_FORMAT, seq_num, ack_num, 0, length, flags)
    checksum = zlib.crc32(temp_header + data) & 0xffffffff 
    
    # Repack with the actual calculated checksum
    header = struct.pack(HEADER_FORMAT, seq_num, ack_num, checksum, length, flags)
    return header + data

def parse_packet(packet):
    """Unpacks the packet and verifies the CRC32 checksum."""
    if len(packet) < HEADER_SIZE:
        return None, False
        
    header = packet[:HEADER_SIZE]
    data = packet[HEADER_SIZE:]
    
    seq_num, ack_num, pkt_checksum, length, flags = struct.unpack(HEADER_FORMAT, header)
    
    # Verify checksum to detect corruption[cite: 1]
    temp_header = struct.pack(HEADER_FORMAT, seq_num, ack_num, 0, length, flags)
    calculated_checksum = zlib.crc32(temp_header + data) & 0xffffffff
    
    is_valid = (pkt_checksum == calculated_checksum)
    return (seq_num, ack_num, length, flags, data), is_valid

def gbn_send_file(filename, udp_socket, dest_addr, window_size=5, timeout=1.0, transfer_type="I", transfer_mode="S"):
    """Reads a file and sends it over UDP using Go-Back-N reliability[cite: 1, 2]."""
    udp_socket.settimeout(timeout) 
    
    with open(filename, "rb") as f:
        file_data = f.read()
        
    if transfer_type == "A":
        # ASCII: convert local newlines to \r\n
        file_data = file_data.replace(b'\r\n', b'\n').replace(b'\n', b'\r\n')
        
    if transfer_mode == "C":
        # Compressed: use zlib to compress the entire payload
        file_data = zlib.compress(file_data)
        
    packets_data = []
    for i in range(0, max(1, len(file_data)), MAX_PAYLOAD):
        chunk = file_data[i:i+MAX_PAYLOAD]
        if not chunk: # Empty file case
            chunk = b""
        packets_data.append(chunk)
            
    total_packets = len(packets_data)
    base = 0
    next_seq_num = 0
    
    print(f"[*] Starting GBN transfer of {total_packets} packets...")
    
    while base < total_packets:
        # Send packets up to the window size limit
        while next_seq_num < base + window_size and next_seq_num < total_packets:
            pkt = make_packet(next_seq_num, 0, FLAG_DATA, packets_data[next_seq_num])
            udp_socket.sendto(pkt, dest_addr)
            next_seq_num += 1
            
        try:
            # Wait for ACKs
            ack_packet, _ = udp_socket.recvfrom(4096)
            parsed_header, is_valid = parse_packet(ack_packet)
            
            if is_valid and parsed_header:
                ack_num = parsed_header[1]
                flags = parsed_header[3]
                
                # Cumulative ACK: Slide the window forward[cite: 2]
                if flags == FLAG_ACK and ack_num >= base:
                    base = ack_num + 1
                    
        except socket.timeout:
            # Timeout! Go-Back-N: Retransmit window[cite: 1, 2]
            print(f"[!] Timeout! Retransmitting window from packet {base}")
            next_seq_num = base 

    # Send FIN packet
    fin_pkt = make_packet(next_seq_num, 0, FLAG_FIN)
    
    # We will try to send the FIN up to 5 times before giving up
    for attempt in range(5):
        udp_socket.sendto(fin_pkt, dest_addr)
        print(f"[*] Sent FIN packet (Attempt {attempt + 1}). Waiting for ACK...")
        
        try:
            ack_packet, _ = udp_socket.recvfrom(4096)
            parsed_header, is_valid = parse_packet(ack_packet)
            
            if is_valid and parsed_header:
                ack_num = parsed_header[1]
                flags = parsed_header[3]
                
                # If we get an ACK for our FIN packet, we are officially done!
                if flags == FLAG_ACK and ack_num >= next_seq_num:
                    print("[*] FIN Acknowledged! File transfer complete.")
                    return # Exit the function successfully
                    
        except socket.timeout:
            print("[!] Timeout waiting for FIN ACK. Retransmitting FIN...")

    print("[!] Transfer finished, but server did not acknowledge FIN.")

def gbn_receive_file(filepath, udp_socket, transfer_type="I", transfer_mode="S"):
    """Receives a file over UDP using Go-Back-N strict ordering."""
    import os
    udp_socket.settimeout(None) 
    expected_seq_num = 0
    received_data = bytearray()
    
    while True:
        packet, addr = udp_socket.recvfrom(4096)
            parsed_header, is_valid = parse_packet(packet)
            
            if not is_valid or not parsed_header:
                continue 
                
            seq_num, _, length, flags, data = parsed_header
            
            if flags == FLAG_FIN:
                ack_pkt = make_packet(0, seq_num, FLAG_ACK)
                udp_socket.sendto(ack_pkt, addr)
                break
                
            if flags == FLAG_DATA and seq_num == expected_seq_num:
                received_data.extend(data)
                # Send Cumulative ACK[cite: 2]
                ack_pkt = make_packet(0, expected_seq_num, FLAG_ACK)
                udp_socket.sendto(ack_pkt, addr)
                expected_seq_num += 1
                
            elif flags == FLAG_DATA:
                # Out of order! Re-ACK the last good packet[cite: 2]
                last_good_ack = max(0, expected_seq_num - 1)
                ack_pkt = make_packet(0, last_good_ack, FLAG_ACK)
                udp_socket.sendto(ack_pkt, addr)

    # Post-process the received data before writing to disk
    final_data = bytes(received_data)
    if transfer_mode == "C":
        try:
            final_data = zlib.decompress(final_data)
        except zlib.error:
            print("[!] Warning: Failed to decompress received data.")
            
    if transfer_type == "A":
        final_data = final_data.replace(b'\r\n', os.linesep.encode())
        
    with open(filepath, "wb") as f:
        f.write(final_data)