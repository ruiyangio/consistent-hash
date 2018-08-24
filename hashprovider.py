from hashlib import sha256

def get_hash_value(input_str):
    encoded = input_str.encode()
    return int.from_bytes(sha256(encoded).digest(), 'big')
