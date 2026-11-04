"""Four-byte network-order length followed by UTF-8 JSON; standard library only."""
import json
import struct

MAX_FRAME_SIZE = 1024 * 1024  # Reject oversized frames before allocating a body.


class ProtocolError(ValueError):
    pass


def recv_exact(sock, count):
    """Return exactly count bytes, or None for EOF before any byte is received.

    EOF after a partial read is a broken frame, not a complete message.
    Socket timeouts and other transport errors propagate to the caller.
    """
    data = bytearray()
    while len(data) < count:
        chunk = sock.recv(count - len(data))
        if not chunk:
            if not data:
                return None
            raise ProtocolError("Connection closed during a frame")
        data.extend(chunk)
    return bytes(data)


def encode_message(message):
    if not isinstance(message, dict):
        raise ProtocolError("Payload must be a JSON object")
    try:
        payload = json.dumps(message, ensure_ascii=False, allow_nan=False).encode("utf-8")
    except (ValueError, TypeError) as exc:
        raise ProtocolError("Message is not valid JSON") from exc
    if not 0 < len(payload) <= MAX_FRAME_SIZE:
        raise ProtocolError("Invalid payload size")
    return struct.pack("!I", len(payload)) + payload


def send_message(sock, message):
    # sendall handles partial writes; send alone may transmit only a prefix.
    sock.sendall(encode_message(message))


def _reject_constant(value):
    raise ValueError("Non-finite JSON number: " + value)


def recv_message(sock):
    header = recv_exact(sock, 4)
    if header is None:
        return None
    length = struct.unpack("!I", header)[0]
    if not 0 < length <= MAX_FRAME_SIZE:
        raise ProtocolError("Invalid payload size")
    payload = recv_exact(sock, length)
    if payload is None:
        raise ProtocolError("Missing frame body")
    try:
        message = json.loads(payload.decode("utf-8"), parse_constant=_reject_constant)
    except (UnicodeError, ValueError, RecursionError) as exc:
        raise ProtocolError("Invalid UTF-8 JSON") from exc
    if not isinstance(message, dict):
        raise ProtocolError("Payload must be a JSON object")
    return message
