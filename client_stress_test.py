"""Twenty concurrent clients: fragmented headers/bodies and merged frames."""
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import socket
import threading
import time
import uuid

from protocol import encode_message, recv_message, send_message


def check(condition, message):
    if not condition:
        raise AssertionError(message)


def worker(index, host, port, run_id, barrier):
    barrier.wait(timeout=15)
    with socket.create_connection((host, port), timeout=30) as conn:
        key = f"{run_id}:{index}"
        value = {"worker": index, "text": "hello café 🌍", "number": index * 10}
        frame = encode_message({"action": "PUT", "key": key, "value": value})
        # Deliberately split both the header and body across writes.
        for start, end in ((0, 1), (1, 3), (3, 7), (7, len(frame))):
            conn.sendall(frame[start:end])
        check(recv_message(conn) == {"status": "OK"}, "PUT failed")
        # Two frames sent together must still be parsed as two messages.
        conn.sendall(encode_message({"action": "GET", "key": key}) +
                     encode_message({"action": "ENQUEUE", "task": key}))
        check(recv_message(conn) == {
              "status": "OK", "value": value}, "GET mismatch")
        check(recv_message(conn) == {"status": "OK"}, "ENQUEUE failed")
        send_message(conn, {"action": "DEQUEUE", "timeout": 1})
        response = recv_message(conn)
        check(response is not None and response.get(
            "status") == "OK", "DEQUEUE failed")
        return response["task"]


def run(host="127.0.0.1", port=9090):
    run_id = uuid.uuid4().hex
    barrier = threading.Barrier(20)
    start = time.perf_counter()
    results = []
    print("[*] Launching 20 concurrent client worker threads...")
    with ThreadPoolExecutor(max_workers=20) as pool:
        futures = {pool.submit(worker, i, host, port,
                               run_id, barrier): i for i in range(20)}
        for future in as_completed(futures):
            # Propagate failures; never print false success.
            results.append(future.result())
            print(
                f"[Worker Thread {futures[future]:02d}] Transaction verified successfully.")
    check(set(results) == {f"{run_id}:{i}" for i in range(20)},
          "Tasks lost, duplicated, or left over from an earlier run; use a fresh server")
    print(
        f"[+] All 20 workers completed execution in {time.perf_counter() - start:.3f} seconds.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=9090)
    args = parser.parse_args()
    run(args.host, args.port)
