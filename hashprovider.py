from hashlib import sha256

def get_hash_value(key):
    return int.from_bytes(sha256(key.encode()).digest(), 'big')
