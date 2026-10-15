import json
import re
import zlib


class InvalidResponse(ValueError):
    pass


def _extract_data_text(raw_line: str) -> str:
    """Extract the exact data array text for CRC verification."""
    match = re.search(r'"data"\s*:\s*', raw_line)
    if match is None:
        raise InvalidResponse("Missing data field")

    start = match.end()
    while start < len(raw_line) and raw_line[start].isspace():
        start += 1

    if start >= len(raw_line) or raw_line[start] != "[":
        raise InvalidResponse("data must be a JSON array")

    depth = 0
    in_string = False
    escaped = False

    for pos in range(start, len(raw_line)):
        char = raw_line[pos]

        if in_string:
            if escaped:
                escaped = False
            elif char == "\\":
                escaped = True
            elif char == '"':
                in_string = False
            continue

        if char == '"':
            in_string = True
        elif char == "[":
            depth += 1
        elif char == "]":
            depth -= 1
            if depth == 0:
                return raw_line[start:pos + 1]

    raise InvalidResponse("Unterminated data array")


def validate_response_line(raw_line: str, expected_sensor: int) -> dict:
    if expected_sensor not in (1, 2):
        raise InvalidResponse("Sensor must be 1 or 2")

    try:
        response = json.loads(raw_line)
    except json.JSONDecodeError as exc:
        raise InvalidResponse("Invalid response JSON") from exc

    if not isinstance(response, dict):
        raise InvalidResponse("Response must be a JSON object")

    required = {
        "session_id", "sensor", "first_index", "count",
        "lost", "session_end", "crc32", "data",
    }
    missing = required - response.keys()
    if missing:
        raise InvalidResponse(f"Missing fields: {sorted(missing)}")

    if not isinstance(response["session_id"], str) or not response["session_id"]:
        raise InvalidResponse("Invalid session_id")

    if type(response["sensor"]) is not int or response["sensor"] != expected_sensor:
        raise InvalidResponse("Sensor ID mismatch")

    first_index = response["first_index"]
    count = response["count"]

    if type(first_index) is not int or first_index < 0:
        raise InvalidResponse("Invalid first_index")
    if type(count) is not int or count < 0:
        raise InvalidResponse("Invalid count")

    if type(response["session_end"]) is not bool:
        raise InvalidResponse("session_end must be boolean")

    lost = response["lost"]
    if not isinstance(lost, list):
        raise InvalidResponse("lost must be a list")

    for item in lost:
        if not isinstance(item, dict):
            raise InvalidResponse("Invalid loss record")
        if type(item.get("from_index")) is not int or item["from_index"] < 0:
            raise InvalidResponse("Invalid loss start index")
        end = item.get("to_index")
        if end is not None and (
            type(end) is not int or end < item["from_index"]
        ):
            raise InvalidResponse("Invalid loss end index")
        if not isinstance(item.get("reason"), str) or not item["reason"]:
            raise InvalidResponse("Invalid loss reason")

    data = response["data"]
    if not isinstance(data, list) or len(data) != count:
        raise InvalidResponse("count does not match data length")

    channels = 3 if expected_sensor == 1 else 2
    minimum, maximum = -(2**23), (2**23) - 1

    for sample in data:
        if not isinstance(sample, list) or len(sample) != channels:
            raise InvalidResponse("Invalid sample channel count")
        for value in sample:
            if type(value) is not int or not minimum <= value <= maximum:
                raise InvalidResponse("Invalid signed 24-bit sample value")

    data_text = _extract_data_text(raw_line)
    actual_crc = zlib.crc32(data_text.encode("utf-8")) & 0xFFFFFFFF

    supplied_crc = response["crc32"]
    if not isinstance(supplied_crc, str) or not re.fullmatch(
        r"[0-9a-fA-F]{8}", supplied_crc
    ):
        raise InvalidResponse("Invalid crc32 format")

    if actual_crc != int(supplied_crc, 16):
        raise InvalidResponse("CRC32 mismatch")

    return {
        **response,
        "values": data,
        "sensor": expected_sensor,
    }
