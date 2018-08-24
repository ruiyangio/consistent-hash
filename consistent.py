from sortedcontainers import SortedDict
from hashprovider import get_hash_value

class ConsistentHash(object):
    def __init__(self, hash_seed = "a", columns = 1, virtual_buckets = 500):
        self.columns = columns
        self.hash_seed = hash_seed
        self.virtual_buckets = virtual_buckets
        self.circle = SortedDict()

        for i in range(self.columns):
            for j in range(self.virtual_buckets):
                hash_value = get_hash_value(hash_seed + str(i) + "-" + str(j))
                self.circle.setdefault(hash_value, i)

    def get_column(self, source):
        hash_value = get_hash_value(source)

        for key in self.circle:
            if key >= hash_value:
                return self.circle[key]
        
        return self.circle.peekitem(0)[1]
