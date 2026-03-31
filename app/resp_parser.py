from typing import List


def _decode_one(data: str, pos: int) -> tuple[List[str], int]:
    """Decode one RESP array starting at pos. Returns (args, new_pos)."""
    if data[pos] != "*":
        raise ValueError(f"Expected '*', got: {data[pos:]!r}")
    nl = data.index("\r\n", pos)
    num_elements = int(data[pos + 1:nl])
    pos = nl + 2

    result = []
    for _ in range(num_elements):
        if data[pos] != "$":
            raise ValueError(f"Expected '$', got: {data[pos:]!r}")
        nl = data.index("\r\n", pos)
        length = int(data[pos + 1:nl])
        pos = nl + 2
        result.append(data[pos:pos + length])
        pos += length + 2  # skip value + \r\n
    return result, pos


def decode_resp(data: str) -> List[str]:
    """Decode a single RESP array of bulk strings into a list of command arguments.

    Example: "*2\r\n$4\r\nECHO\r\n$5\r\nhello\r\n" -> ["ECHO", "hello"]
    """
    result, _ = _decode_one(data, 0)
    return result


def decode_resp_all(data: str) -> tuple[list[list[str]], int]:
    """Decode all complete RESP arrays from data. Returns (commands, bytes_consumed)."""
    commands = []
    pos = 0
    while pos < len(data):
        if data[pos] != "*":
            break
        try:
            result, pos = _decode_one(data, pos)
            commands.append(result)
        except (ValueError, IndexError):
            break  # incomplete command
    return commands, pos


def bulk_xread_response(streams: list[tuple[str, list[tuple[str, dict[str, str]]]]]) -> bytes:
    """Encode XREAD response: array of [key, entries] pairs."""
    parts = [b"*" + str(len(streams)).encode() + b"\r\n"]
    for key, entries in streams:
        parts.append(b"*2\r\n")
        parts.append(bulk_string(key))
        parts.append(bulk_stream_entries(entries))
    return b"".join(parts)


def bulk_stream_entries(entries: list[tuple[str, dict[str, str]]]) -> bytes:
    """Encode a list of stream entries as a RESP array of [id, [field, value, ...]] pairs."""
    parts = [b"*" + str(len(entries)).encode() + b"\r\n"]
    for entry_id, fields in entries:
        flat_fields = [item for pair in fields.items() for item in pair]
        parts.append(b"*2\r\n")
        parts.append(bulk_string(entry_id))
        parts.append(bulk_array(flat_fields))
    return b"".join(parts)


def bulk_array(values: list[str]) -> bytes:
    header = b"*" + str(len(values)).encode() + b"\r\n"
    return header + b"".join(bulk_string(v) for v in values)


def bulk_int(value: int) -> bytes:
    return b":" + str(value).encode() + b"\r\n"


def bulk_string(value: str) -> bytes:
    encoded = value.encode()
    return b"$" + str(len(encoded)).encode() + b"\r\n" + encoded + b"\r\n"
