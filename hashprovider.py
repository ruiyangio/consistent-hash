from hashlib import sha256

MAX_HASH_PLUS_ONE = 2**(sha256().digest_size * 4)

def get_hash_value(input_str):
    encoded = input_str.encode()
    hash_digest = sha256(encoded).digest()
    return int.from_bytes(hash_digest, 'big')
