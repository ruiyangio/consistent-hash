from consistent import ConsistentHash

class Item(object):
    def __init__(self, id, name, hash_key, v_bucket_key=None):
        self.id = id
        self.name = name
        self.hash_key = hash_key
        self.v_bucket_key = v_bucket_key

class Node(object):
    def __init__(self, id):
        self.id = id
        self.items = {}
        self.v_bucket_map = {}

    def add_item(self, item):
        self.items[item.id] = item
        if item.v_bucket_key not in self.v_bucket_map:
            self.v_bucket_map[item.v_bucket_key] = set()
        self.v_bucket_map[item.v_bucket_key].add(item.id)
    
    def remove_item(self, item_id):
        v_bucket_key = self.items[item_id].v_bucket_key
        self.v_bucket_map[v_bucket_key].remove(item_id)
        self.items.pop(item_id)

    def add_v_bucket(self, v_bucket_key, items):
        self.v_bucket_map[v_bucket_key] = set()
        for item in items:
            self.items[item.id] = item
            self.v_bucket_map[v_bucket_key].add(item.id)

    def remove_v_buckets(self, v_bucket_keys):
        removed_items = []
        for v_bucket_key in v_bucket_keys:
            if v_bucket_key not in self.v_bucket_map:
                continue
            for item_id in self.v_bucket_map[v_bucket_key]:
                if item_id not in self.items:
                    continue
                removed_items.append(self.items.pop(item_id))
        return removed_items

class Cluster(object):
    def __init__(self, n=1, v_buckets=1000):
        self.n = n
        self.nodes = {}
        self.meta = ConsistentHash("a", n, v_buckets)

        for column_id in self.meta.column_ids:
            self.nodes[column_id] = Node(column_id)

    def get_item_dist(self):
        return [ len(node.items) for node in self.nodes.values() ]
    
    def insert_item(self, item):
        column_id, v_bucket_key = self.meta.get_column_with_hash_key(item.hash_key)
        item.v_bucket_key = v_bucket_key
        self.nodes[column_id].add_item(item)

    def add_node_and_rebalance(self):
        self.n += 1
        new_column_id, v_bucket_to_move = self.meta.add_column()
        self.nodes[new_column_id] = Node(new_column_id)
        # Rebalance
        for column_id in v_bucket_to_move:
            removed_items = self.nodes[column_id].remove_v_buckets(v_bucket_to_move[column_id])
            for item in removed_items:
                self.nodes[new_column_id].add_item(item)
    
    def remove_node_and_rebalance(self, node_id):
        self.n -= 1
        v_bucket_to_move = self.meta.remove_column(node_id)
        # Rebalance
        for column_id in v_bucket_to_move:
            removed_items = self.nodes[node_id].remove_v_buckets(v_bucket_to_move[column_id])
            for item in removed_items:
                self.nodes[column_id].add_item(item)
        self.nodes.pop(column_id)
