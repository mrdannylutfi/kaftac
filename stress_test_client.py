import socket
import struct
import time
import uuid
import random

TARGET_HOST = "127.0.0.1"
TARGET_PORT = 8080

def create_length_prefixed_frame(msg_id: str, data_payload: str) -> bytes:
    """
    Constructs a structural Length-Prefixed Binary Frame matching the C Spec.
    Format: [4-byte big-endian length] + [raw payload bytes]
    """
    # Create a simple message structure combining id and string body
    unified_payload = f"{msg_id}:{data_payload}".encode('utf-8')
    payload_len = len(unified_payload)
    
    # '!I' enforces Network Byte Order (Big Endian) 4-byte unsigned int
    header = struct.pack('!I', payload_len)
    return header + unified_payload

def run_stress_test():
    print(f"Connecting to C Socket Target Engine at {TARGET_HOST}:{TARGET_PORT}...")
    
    try:
        client_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client_sock.connect((TARGET_HOST, TARGET_PORT))
        print("Network socket pipeline successfully attached. Starting simulation loops.")
    except ConnectionRefusedError:
        print(f"Error: Could not connect to C socket engine on port {TARGET_PORT}. Ensure it is compiled and actively running.")
        return

    message_count = 0
    start_time = time.time()

    try:
        while True:
            # 95% of the time: Simulate baseline throughput (e.g., ~200 msgs/sec)
            # 5% of the time: Trigger a massive burst window (e.g., 25,000 msgs/sec spike)
            is_burst_scenario = (random.random() < 0.05)
            
            if not is_burst_scenario:
                # Baseline Phase: Send small incremental batches
                for _ in range(20):
                    msg_id = str(uuid.uuid4())[:8]
                    frame = create_length_prefixed_frame(msg_id, f"BASELINE_TELEMETRY_DATA_CHUNK_{message_count}")
                    client_sock.sendall(frame)
                    message_count += 1
                time.sleep(0.1) # 100ms pacing gap
                
            else:
                # 5% Burst Spike Phase: Saturate the C Epoll queue as quickly as possible
                print(f"\n⚡ BURST DETECTED! Streaming high-velocity payload bursts...")
                burst_buffer = bytearray()
                
                # Coalesce 10,000 frames locally into memory before hitting the network interface
                for _ in range(10000):
                    msg_id = str(uuid.uuid4())[:8]
                    frame = create_length_prefixed_frame(msg_id, f"BURST_SPIKE_LOAD_METRIC_PACKET_{message_count}")
                    burst_buffer.extend(frame)
                    message_count += 1
                
                # Single massive continuous socket block transfer
                client_sock.sendall(burst_buffer)
                print(f"✔ Transmitted 10,000 message burst frame packet to C engine.")
                time.sleep(0.01) # Negligible backoff before reassessing channel status

            # Status output tracking telemetry every 2 seconds
            if message_count % 5000 == 0 or (time.time() - start_time) > 2.0:
                elapsed = time.time() - start_time
                print(f"⚙ Processing Progress: Generated {message_count} unique binary frames. Run duration: {elapsed:.2f}s")
                start_time = time.time()

    except KeyboardInterrupt:
        print("\nStopping traffic generation client.")
    finally:
        client_sock.close()
        print("Socket connection released safely.")

if __name__ == "__main__":
    run_stress_test()
