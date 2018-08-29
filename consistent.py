import util
import random
import uuid
from sortedcontainers import SortedDict
from hashprovider import get_hash_value

class ConsistentHash(object):
    def __init__(self, hash_seed = "a", columns = 1, virtual_buckets = 1000):
        self.columns = columns
        self.column_ids = [ uuid.uuid4().hex for i in range(self.columns) ]
        self.hash_seed = hash_seed
        self.virtual_buckets = virtual_buckets
        self.total_v_buckets = columns * virtual_buckets
        self.circle = SortedDict()
        self.v_buckets_map = dict()

        for i, column_id in enumerate(self.column_ids):
            if column_id not in self.v_buckets_map:
                self.v_buckets_map[column_id] = set()
    
            for j in range(self.virtual_buckets):
                hash_value = get_hash_value(hash_seed, str(i), str(j))
                self.circle.setdefault(hash_value, column_id)
                self.v_buckets_map[column_id].add(hash_value)      

    def add_column(self):
        bucket_to_move_per_col = self.total_v_buckets // (self.columns * (self.columns + 1))
        self.columns += 1
        new_column_id = uuid.uuid4()
        self.column_ids.append(new_column_id)

        self.v_buckets_map[new_column_id] = set()
        moved_bucket_keys = {}

        for column_id in self.v_buckets_map: 
            v_bucket_keys = random.sample(self.v_buckets_map[column_id], bucket_to_move_per_col)
            moved_bucket_keys[column_id] = v_bucket_keys
            
            for v_bucket_key in v_bucket_keys:
                self.circle[v_bucket_key] = new_column_id
                self.v_buckets_map[column_id].remove(v_bucket_key)
                self.v_buckets_map[new_column_id].add(v_bucket_key)

        return (new_column_id, moved_bucket_keys)

    def remove_column(self, column_id):
        

    def get_column(self, source):
        hash_key = get_hash_value(source, source[0], source[-1])
        return self.get_column_with_hash_key(hash_key)

    def get_column_with_hash_key(self, hash_key):
        key = util.find_first_ge(self.circle.keys(), hash_key)
        if key != -1:
            return (self.circle[key], key)
        return (self.circle.peekitem(0)[1], -1)
