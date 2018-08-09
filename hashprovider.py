from hashlib import sha512

MAX_HASH_PLUS_ONE = 2**(sha512().digest_size * 8)

def get_hash_value(input_str):
    encoded = input_str.encode()
    hash_digest = sha512(encoded).digest()
    return int.from_bytes(hash_digest, 'big')
