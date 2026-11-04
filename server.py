"""Concurrent TCP server. Run with: python server.py"""
import argparse
from concurrent.futures import ThreadPoolExecutor
import math
import socket
import threading

from protocol import ProtocolError, recv_message, send_message
from storage import Storage

MAX_WORKERS = 10
MAX_PENDING = 20
CLIENT_TIMEOUT = 5
MAX_DEQUEUE_WAIT = 2


def dispatch(store, request):
    action = request.get("action")
    if action in ("PUT", "GET"):
        key = request.get("key")
        if not isinstance(key, str) or not key:
            return {"status": "ERROR", "error": "key must be a nonempty string"}
        if action == "PUT":
            if "value" not in request:
                return {"status": "ERROR", "error": "PUT requires value"}
            store.put(key, request["value"])
            return {"status": "OK"}
        found, value = store.get(key)
        return {"status": "OK", "value": value} if found else {"status": "NOT_FOUND"}
    if action == "ENQUEUE":
        if "task" not in request:
            return {"status": "ERROR", "error": "ENQUEUE requires task"}
        store.enqueue(request["task"])
        return {"status": "OK"}
    if action == "DEQUEUE":
        timeout = request.get("timeout", 0)
        if (type(timeout) not in (int, float) or not math.isfinite(timeout)
                or not 0 <= timeout <= MAX_DEQUEUE_WAIT):
            return {"status": "ERROR", "error": "timeout must be between 0 and 2 seconds"}
        found, task = store.dequeue(timeout)
        return {"status": "OK", "task": task} if found else {"status": "EMPTY"}
    return {"status": "ERROR", "error": "Unknown action"}


def handle_client(conn, store, slots):
    try:
        with conn:
            conn.settimeout(CLIENT_TIMEOUT)
            while True:
                try:
                    request = recv_message(conn)
                    if request is None:
                        return
                    send_message(conn, dispatch(store, request))
                except ProtocolError as exc:
                    # A malformed stream cannot safely be resumed.
                    send_message(conn, {"status": "ERROR", "error": str(exc)})
                    return
    except (OSError, RecursionError):
        # Abrupt disconnects, timeouts, and excessively nested payloads only
        # terminate this connection, not the accept loop or other clients.
        pass
    finally:
        slots.release()


def serve(host="0.0.0.0", port=9090, stop_event=None):
    store = Storage()
    stop_event = stop_event or threading.Event()
    # ThreadPoolExecutor bounds running workers but NOT its internal queue.
    # This semaphore also bounds accepted running + pending connections.
    slots = threading.BoundedSemaphore(MAX_WORKERS + MAX_PENDING)
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
            listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            listener.bind((host, port))
            listener.listen(MAX_WORKERS + MAX_PENDING)
            listener.settimeout(0.5)
            print(f"[*] Server initialized on {host}:{port}", flush=True)
            print(f"[*] Thread Pool initialized with max_workers={MAX_WORKERS}...", flush=True)
            try:
                while not stop_event.is_set():
                    if not slots.acquire(timeout=0.5):
                        continue
                    try:
                        conn, _ = listener.accept()
                    except socket.timeout:
                        slots.release()
                        continue
                    except BaseException:
                        slots.release()
                        raise
                    try:
                        pool.submit(handle_client, conn, store, slots)
                    except BaseException:
                        conn.close()
                        slots.release()
                        raise
            except KeyboardInterrupt:
                print("\n[*] Closing server; waiting for client handlers...", flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=9090)
    args = parser.parse_args()
    serve(args.host, args.port)
