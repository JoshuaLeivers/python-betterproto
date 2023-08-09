from tests.output_betterproto.oneof import Test as OneofTest
from tests.output_betterproto.nested import Test as NestedTest, TestMsg as NestedTestMsg

from betterproto import dump_varint, load_varint

from subprocess import run
from shutil import which
from pathlib import Path

import pytest


correct_oneof = OneofTest().from_dict(
    {"pitied": 1, "just_a_regular_field": 123456789, "bar_name": "Testing"}
)

correct_nested = NestedTest().from_dict(
    {
        "nested": {"count": 1},
        "sibling": {"foo": 2},
        "sibling2": {"foo": 3},
        "msg": NestedTestMsg.THIS,
    }
)


@pytest.fixture(scope="module")
@pytest.mark.skipif(which("mvn") is None, "Maven is absent and is required")
@pytest.mark.skipif(which("jar") is None, "`jar` command is absent and is required")
def compile_jar(output_dir):
    # Compile the JAR
    proc_maven = run(["mvn", "clean", "install", "-f", "tests/streams/java/pom.xml"])
    if proc_maven.returncode != 0:
        pytest.skip("Maven compatibility-test.jar build failed")


@pytest.fixture(scope="module")
def output_dir():
    path = Path("tests/output_streams/")
    if not path.exists():
        path.mkdir(parents=True)


def test_single_varint(compile_jar):
    single_byte = 1
    multi_byte = 123456789

    # Write a single-byte varint to a file and have Java read it back
    returned = run_java_single_varint(single_byte)
    assert returned == single_byte

    # Same for a multi-byte varint
    returned = run_java_single_varint(multi_byte)
    assert returned == multi_byte


def run_java_single_varint(value: int) -> int:
    # Write single varint to file
    with open("tests/output_streams/py_single_varint.out", "wb") as stream:
        dump_varint(value, stream)
        stream.flush()

    # Have Java read this varint and write it back
    run(["java", "-jar", "tests/compatibility-test.jar", "single_varint"], check=True)

    # Read single varint from Java output file
    with open("tests/output_streams/java_single_varint.out", "rb") as stream:
        returned = load_varint(stream)
        with pytest.raises(EOFError):
            load_varint(stream)

    return returned


def test_multiple_varints(compile_jar):
    single_byte = 1
    multi_byte = 123456789
    over32 = 3000000000

    # Write two varints to the same file
    with open("tests/output_streams/py_multiple_varints.out", "wb") as stream:
        dump_varint(single_byte, stream)
        dump_varint(multi_byte, stream)
        dump_varint(over32, stream)
        stream.flush()

    # Have Java read these varints and write them back
    run(["java", "-jar", "tests/compatibility-test.jar", "multiple_varints"], check=True)

    # Read varints from Java output file
    with open("tests/output_streams/java_multiple_varints.out", "rb") as stream:
        returned_single = load_varint(stream)
        returned_multi = load_varint(stream)
        returned_over32 = load_varint(stream)
        with pytest.raises(EOFError):
            load_varint(stream)

    assert returned_single == single_byte
    assert returned_multi == multi_byte
    assert returned_over32 == over32


def test_single_message(compile_jar):
    # Write message to file
    with open("tests/output_streams/py_single_message.out", "wb") as stream:
        correct_oneof.to_stream(stream)

    # Have Java read and return the message
    run(["java", "-jar", "tests/compatibility-test.jar", "single_message"], check=True)

    # Read and check the returned message
    with open("tests/output_streams/java_single_message.out", "rb") as stream:
        returned = OneofTest().from_stream(stream, len(bytes(correct_oneof)))
        with pytest.raises(EOFError):
            OneofTest().from_stream(stream)

    assert returned == correct_oneof


def test_multiple_messages(compile_jar):
    # Write delimited messages to file
    with open("tests/output_streams/py_multiple_messages.out", "wb") as stream:
        correct_oneof.to_stream_delimited(stream)
        correct_nested.to_stream_delimited(stream)
        stream.flush()

    # Have Java read and return the messages
    run(["java", "-jar", "tests/compatibility-test.jar", "multiple_messages"], check=True)

    # Read and check the returned messages
    with open("tests/output_streams/java_multiple_messages.out", "rb") as stream:
        returned_oneof = OneofTest().from_stream_delimited(stream)
        returned_nested = NestedTest().from_stream_delimited(stream)
        with pytest.raises(EOFError):
            NestedTest().from_stream_delimited(stream)

    assert returned_oneof == correct_oneof
    assert returned_nested == correct_nested
