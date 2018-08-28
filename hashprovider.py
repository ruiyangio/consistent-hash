from hashlib import sha256

def get_hash_value(key, salt, pepper):
    seed = key + salt + "-" + pepper
    return int.from_bytes(sha256(seed.encode()).digest(), 'big')
