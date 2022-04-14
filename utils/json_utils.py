import collections
from collections.abc import MutableMapping, Mapping


def extract_json_sequence(data, key_sequence):
    """
    Extracts a value from a nested dictionary using a sequence of keys.
    """
    if len(key_sequence) == 1:
        return data[key_sequence[0]]
    else:
        return extract_json_sequence(data[key_sequence[0]], key_sequence[1:])



def extract_json_with_default(data, key_sequence, default=None):
    """
    Extracts a value from a nested dictionary using a sequence of keys.
    """
    if len(key_sequence) == 1:
        try:
            return data[key_sequence[0]]
        except KeyError:
            return default
        except TypeError:
            return default
    else:
        try:
            return extract_json_with_default(data[key_sequence[0]], key_sequence[1:], default=default)
        except KeyError:
            return default


def parse_json(data, schema):
    """
    Parses a JSON object using a schema.
    """
    parsed_data = {}
    for key, value in schema.items():
        key_sequence = value["path"]
        parsed_data[key] = extract_json_with_default(data, key_sequence, default=None)
    return parsed_data



def flatten_json(data, prefix=None):
    result = collections.defaultdict(list)
    for k, v in data.items():
        if isinstance(v, Mapping):
            result.update(flatten_json(v, prefix=k))
        else:
            result[prefix + "." + k].append(v)
    return dict(result)
    

def flatten_nested_dict(d, parent_key='', sep='_'):
    items = []
    for k, v in d.items():
        new_key = parent_key + sep + k if parent_key else k
        if isinstance(v, MutableMapping):
            items.extend(flatten_nested_dict(v, new_key, sep=sep).items())
        else:
            items.append((new_key, v))
    return dict(items)



