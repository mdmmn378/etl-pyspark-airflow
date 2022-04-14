from utils.json_utils import *
import pytest


def test_extract_json_sequence():
    data = {
        "a": {
            "b": {
                "c": "d"
            }
        }
    }
    key_sequence = ["a", "b", "c"]
    assert extract_json_sequence(data, key_sequence) == "d"
    with pytest.raises(KeyError):
        extract_json_sequence(data, ["a", "x"])


def test_extract_json_with_default():
    data = {
        "a": {
            "b": {
                "c": "d"
            }
        }
    }
    key_sequence = ["a", "b", "c"]
    assert extract_json_with_default(data, key_sequence) == "d"
    assert extract_json_with_default(data, ["a", "x"]) == None
    assert extract_json_with_default(data, ["z", "x"]) == None



def test_parse_json(data, schema):
    print(parse_json(data, schema))
    assert parse_json(data, schema)


def test_flatten_json(data, prefix="_"):
    print(flatten_json(data, prefix))
    assert flatten_json(data, prefix)


def test_flatten_nested_dict(data, parent_key="", sep="_"):
    print(flatten_nested_dict(data, parent_key, sep))
    assert flatten_nested_dict(data, parent_key, sep)