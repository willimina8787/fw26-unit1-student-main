"""Extra regression checks. Run: python -m unittest -v test_unit1.py"""
from concurrent.futures import ThreadPoolExecutor
import socket
import struct
import threading
import time
import unittest

from client_stress_test import run
from protocol import MAX_FRAME_SIZE, ProtocolError, encode_message, recv_exact, recv_message, send_message
from server import serve
from storage import Storage


class FragmentSocket:
    def __init__(self, data):
        self.data = bytearray(data)

    def recv(self, count):
        # Deterministic fragmentation even when the OS would coalesce sends.
        result = bytes(self.data[:min(count, 1)])
        del self.data[:len(result)]
        return result


class ProtocolTests(unittest.TestCase):
    def test_fragmentation_and_back_to_back_frames(self):
        msg = {"value": "café 🌍"}
        stream = FragmentSocket(encode_message(msg) + encode_message({}))
        self.assertEqual(recv_message(stream), msg)
        self.assertEqual(recv_message(stream), {})
        self.assertIsNone(recv_message(stream))

    def test_truncated_and_invalid_frames(self):
        cases = [b"\x00", struct.pack("!I", 2), struct.pack("!I", 3) + b"{}",
                 struct.pack("!I", 0), struct.pack("!I", MAX_FRAME_SIZE + 1)]
        for payload in (b"bad", b"[]", b"\xff", b'{"x": NaN}'):
            cases.append(struct.pack("!I", len(payload)) + payload)
        for frame in cases:
            with self.subTest(frame=frame):
                with self.assertRaises(ProtocolError):
                    recv_message(FragmentSocket(frame))

    def test_exact_and_byte_length(self):
        self.assertEqual(recv_exact(FragmentSocket(b"abc"), 3), b"abc")
        frame = encode_message({"text": "🌍"})
        self.assertEqual(struct.unpack("!I", frame[:4])[0], len(frame) - 4)


class StorageTests(unittest.TestCase):
    def test_fifo_and_null(self):
        store = Storage()
        store.put("null", None)
        self.assertEqual(store.get("null"), (True, None))
        self.assertEqual(store.get("missing"), (False, None))
        store.enqueue(None)
        store.enqueue(2)
        self.assertEqual(store.dequeue(), (True, None))
        self.assertEqual(store.dequeue(), (True, 2))
        self.assertEqual(store.dequeue(0.01), (False, None))

    def test_waiter_is_notified(self):
        store = Storage()
        with ThreadPoolExecutor(max_workers=1) as pool:
            result = pool.submit(store.dequeue, 2)
            time.sleep(0.02)
            store.enqueue("task")
            self.assertEqual(result.result(timeout=3), (True, "task"))


class ServerTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        with socket.socket() as probe:
            probe.bind(("127.0.0.1", 0))
            cls.port = probe.getsockname()[1]
        cls.stop = threading.Event()
        cls.thread = threading.Thread(target=serve, args=("127.0.0.1", cls.port, cls.stop))
        cls.thread.start()
        for _ in range(100):
            try:
                with cls.connect():
                    return
            except OSError:
                time.sleep(0.02)
        raise RuntimeError("Server did not start")

    @classmethod
    def connect(cls):
        return socket.create_connection(("127.0.0.1", cls.port), timeout=10)

    @classmethod
    def tearDownClass(cls):
        cls.stop.set()
        cls.thread.join(timeout=15)
        if cls.thread.is_alive():
            raise RuntimeError("Server failed to stop")

    def test_20_clients(self):
        run("127.0.0.1", self.port)

    def test_disconnects_and_bad_requests_do_not_kill_server(self):
        for prefix in (b"\x00", struct.pack("!I", 100) + b"{"):
            with self.connect() as conn:
                conn.sendall(prefix)
        with self.connect() as conn:
            conn.sendall(struct.pack("!I", MAX_FRAME_SIZE + 1))
            self.assertEqual(recv_message(conn)["status"], "ERROR")
            self.assertIsNone(recv_message(conn))
        with self.connect() as conn:
            for request in ({"action": "BAD"}, {"action": "PUT", "key": "x"},
                            {"action": "GET", "key": []},
                            {"action": "DEQUEUE", "timeout": -1}):
                send_message(conn, request)
                self.assertEqual(recv_message(conn)["status"], "ERROR")
            send_message(conn, {"action": "GET", "key": "missing"})
            self.assertEqual(recv_message(conn), {"status": "NOT_FOUND"})

    def test_all_workers_waiting_eventually_release(self):
        # Ten empty dequeues must time out so queued producers can progress.
        barrier = threading.Barrier(10)

        def empty_waiter():
            with self.connect() as conn:
                barrier.wait(timeout=5)
                send_message(conn, {"action": "DEQUEUE", "timeout": 0.1})
                return recv_message(conn)

        with ThreadPoolExecutor(max_workers=10) as pool:
            responses = list(pool.map(lambda _: empty_waiter(), range(10)))
        self.assertEqual(responses, [{"status": "EMPTY"}] * 10)
        with self.connect() as conn:
            send_message(conn, {"action": "ENQUEUE", "task": "after-wait"})
            self.assertEqual(recv_message(conn), {"status": "OK"})
            send_message(conn, {"action": "DEQUEUE"})
            self.assertEqual(recv_message(conn), {"status": "OK", "task": "after-wait"})


if __name__ == "__main__":
    unittest.main()
