import util
import random
from sortedcontainers import SortedDict
from hashprovider import get_hash_value

class ConsistentHash(object):
    def __init__(self, hash_seed = "a", columns = 1, virtual_buckets = 1000):
        self.columns = columns
        self.hash_seed = hash_seed
        self.virtual_buckets = virtual_buckets
        self.total_v_buckets = columns * virtual_buckets
        self.circle = SortedDict()
        self.v_buckets_map = dict()

        for i in range(self.columns):
            if i not in self.v_buckets_map:
                self.v_buckets_map[i] = set()
    
            for j in range(self.virtual_buckets):
                hash_value = get_hash_value(hash_seed, str(i), str(j))
                self.circle.setdefault(hash_value, i)
                self.v_buckets_map[i].add(hash_value)      

    def add_column(self):
        bucket_to_move_per_col = self.total_v_buckets // (self.columns * (self.columns + 1))
        self.columns += 1
        last_column = self.columns - 1

        self.v_buckets_map[last_column] = set()
        moved_bucket_keys = {}

        for column in self.v_buckets_map: 
            v_bucket_keys = random.sample(self.v_buckets_map[column], bucket_to_move_per_col)
            moved_bucket_keys[column] = v_bucket_keys
            
            for v_bucket_key in v_bucket_keys:
                self.circle[v_bucket_key] = last_column
                self.v_buckets_map[column].remove(v_bucket_key)
                self.v_buckets_map[last_column].add(v_bucket_key)

        return moved_bucket_keys

    def get_column(self, source):
        hash_key = get_hash_value(source, source[0], source[-1])
        return self.get_column_with_hash_key(hash_key)

    def get_column_with_hash_key(self, hash_key):
        key = util.find_first_ge(self.circle.keys(), hash_key)
        if key != -1:
            return (self.circle[key], key)
        return (self.circle.peekitem(0)[1], -1)
