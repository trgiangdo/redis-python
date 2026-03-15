from typing import List


def decode_resp(data: str) -> List[str]:
    """Decode a RESP array of bulk strings into a list of command arguments.

    Example: "*2\r\n$4\r\nECHO\r\n$5\r\nhello\r\n" -> ["ECHO", "hello"]
    """
    lines = data.split("\r\n")
    if not lines or not lines[0].startswith("*"):
        raise ValueError(f"Expected RESP array, got: {data!r}")

    num_elements = int(lines[0][1:])
    result = []
    i = 1

    for _ in range(num_elements):
        if not lines[i].startswith("$"):
            raise ValueError(f"Expected bulk string, got: {lines[i]!r}")
        length = int(lines[i][1:])
        i += 1
        result.append(lines[i][:length])
        i += 1

    return result


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
