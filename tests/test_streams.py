from tests.output_betterproto.oneof import Test as OneofTest
from tests.output_betterproto.nested import Test as NestedTest, TestMsg as NestedTestMsg

from betterproto import load_varint, encode_varint, dump_varint

import pytest
from pathlib import Path

oneof_example = OneofTest().from_dict(
    {"pitied": 1, "just_a_regular_field": 123456789, "bar_name": "Testing"}
)

nested_example = NestedTest().from_dict(
    {
        "nested": {"count": 1},
        "sibling": {"foo": 2},
        "sibling2": {"foo": 3},
        "msg": NestedTestMsg.THIS,
    }
)


# Inputs


def test_load_varint():
    with open("tests/streams/varints.in", "rb") as stream:
        assert load_varint(stream) == 1  # Single-byte Int32
        assert load_varint(stream) == 123456789  # Multi-byte Int32
        assert load_varint(stream) == 1  # Single-byte Int64
        assert load_varint(stream) == 3000000000  # Multi-byte, 33+ bits Int64
        assert (
            load_varint(stream) == 18446744073709551615
        )  # 64-bit maximum integer Int64
        with pytest.raises(EOFError):
            load_varint(stream)  # End of stream


def test_load_varint_cutoff():
    with open("tests/streams/cutoff_varints_single.in", "rb") as stream, pytest.raises(
        ValueError
    ):
        load_varint(stream)

    with open(
        "tests/streams/cutoff_varints_multiple.in", "rb"
    ) as stream, pytest.raises(ValueError):
        load_varint(stream)


def test_from_stream():
    with open("tests/streams/multiple.in", "rb") as stream:
        stream.read(1)  # Skip message size header

        assert OneofTest().from_stream(stream, 16) == oneof_example


def test_from_stream_stream_empty():
    with open("tests/streams/empty.in", "rb") as stream, \
            pytest.raises(EOFError):
        OneofTest().from_stream(stream)


def test_from_stream_stream_empty_unexpected():
    with open("tests/streams/empty.in", "rb") as stream, pytest.raises(ValueError):
        assert OneofTest().from_stream(stream, 16)


def test_from_stream_delimited():
    with open("tests/streams/multiple.in", "rb") as stream:
        assert OneofTest().from_stream_delimited(stream) == oneof_example
        assert NestedTest().from_stream_delimited(stream) == nested_example
        with pytest.raises(EOFError):
            NestedTest().from_stream_delimited(stream)


# Outputs


@pytest.fixture(scope="module")
def output_dir():
    Path("tests/output_streams").mkdir(parents=True, exist_ok=True)


def test_dump_varint(output_dir):
    single_byte = 1
    multi_byte = 123456789
    over32 = 3000000000
    maximum = 18446744073709551615

    # Test single-byte write capabilities
    with open("tests/output_streams/dump_varint.out", "wb") as stream:
        dump_varint(single_byte, stream)

    with open("tests/output_streams/dump_varint.out", "rb") as stream:
        assert stream.read() == encode_varint(single_byte)

    # Test multi-byte write capabilities
    with open("tests/output_streams/dump_varint.out", "wb") as stream:
        dump_varint(multi_byte, stream)

    with open("tests/output_streams/dump_varint.out", "rb") as stream:
        assert stream.read() == encode_varint(multi_byte)

    # Test 33+-bit write capabilities
    with open("tests/output_streams/dump_varint.out", "wb") as stream:
        dump_varint(over32, stream)

    with open("tests/output_streams/dump_varint.out", "rb") as stream:
        assert stream.read() == encode_varint(over32)

    # Test 64-bit unsigned integer limit write capabilities
    with open("tests/output_streams/dump_varint.out", "wb") as stream:
        dump_varint(maximum, stream)

    with open("tests/output_streams/dump_varint.out", "rb") as stream:
        assert stream.read() == encode_varint(maximum)


def test_dump_varint_multiple(output_dir):
    with open("tests/output_streams/dump_varint_multiple.out", "wb") as stream:
        # Write the same contents as varints.in to a file
        dump_varint(1, stream)
        dump_varint(123456789, stream)
        dump_varint(1, stream)
        dump_varint(3000000000, stream)
        dump_varint(18446744073709551615, stream)

    # Check that both files have the same contents, as expected
    with open("tests/output_streams/dump_varint_multiple.out", "rb") as testing, open(
        "tests/streams/varints.in", "rb"
    ) as expected:
        assert testing.read() == expected.read()


def test_message_to_stream(output_dir):
    with open("tests/output_streams/write_message.out", "wb") as stream:
        # Write test message to file
        stream.write(b"\x10")  # Message size prefix, same as given in multiple.in
        oneof_example.to_stream(stream)

    # Check written message against known-good wire of the same message
    with open("tests/output_streams/write_message.out", "rb") as testing, open(
        "tests/streams/multiple.in", "rb"
    ) as expected:
        assert testing.read(17) == expected.read(17)


def test_message_to_stream_multiple(output_dir):
    with open("tests/output_streams/write_message_multiple.out", "wb") as stream:
        # Write test messages to file separately
        stream.write(b"\x10")  # Message size prefix, same as given in multiple.in
        oneof_example.to_stream(stream)
        stream.write(b"\x0E")  # Message size prefix, same as given in multiple.in
        nested_example.to_stream(stream)

    # Check written messages against known-good file of the same data
    with open("tests/output_streams/write_message_multiple.out", "rb") as testing, open(
        "tests/streams/multiple.in", "rb"
    ) as expected:
        assert testing.read() == expected.read()


def test_message_to_stream_delimited(output_dir):
    with open("tests/output_streams/delimited_messages.out", "wb") as stream:
        # Write test messages to file
        oneof_example.to_stream_delimited(stream)
        nested_example.to_stream_delimited(stream)

    # Check written file against test file that should match
    with open("tests/output_streams/delimited_messages.out", "rb") as testing, open(
        "tests/streams/multiple.in", "rb"
    ) as expected:
        assert testing.read() == expected.read()
