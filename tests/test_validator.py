import json
import zlib

import pytest

from heartbeat_pipeline.validator import (
    InvalidResponse,
    validate_response_line,
)


def make_line(data_text="[[1,2,3]]", sensor=1, crc_override=None):
    checksum = zlib.crc32(data_text.encode("utf-8")) & 0xFFFFFFFF
    if crc_override is not None:
        checksum = crc_override

    # Preserve the exact array text in the wire response.
    return (
        '{"session_id":"session-1",'
        f'"sensor":{sensor},'
        '"first_index":10,"count":1,"lost":[],'
        '"session_end":false,'
        f'"crc32":"{checksum:08x}","data":{data_text}' 
        '}'
    )


def test_valid_response_uses_documented_format():
    line = make_line()

    result = validate_response_line(line, expected_sensor=1)

    assert result["values"] == [[1, 2, 3]]
    assert result["first_index"] == 10


def test_corrupt_crc_is_rejected():
    line = make_line(crc_override=0)

    with pytest.raises(InvalidResponse, match="CRC32 mismatch"):
        validate_response_line(line, expected_sensor=1)


def test_wrong_channel_count_is_rejected():
    line = make_line(data_text="[[1,2]]")

    with pytest.raises(InvalidResponse, match="channel count"):
        validate_response_line(line, expected_sensor=1)
