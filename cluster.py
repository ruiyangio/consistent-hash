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
                removed_items.append(self.items.pop(item_id))
        return removed_items

class Cluster(object):
    def __init__(self, n=1):
        self.nodes = []
        self.n = n

        for i in range(self.n):
            self.nodes.append(Node(i))

    def add_node(self):
        self.nodes.append(Node(self.n))
        self.n += 1

    def get_item_dist(self):
        return [ len(node.items) for node in self.nodes ]

    def add_node_and_rebalance(self, hash_meta):
        self.add_node()
        v_bucket_to_move = hash_meta.add_column()
        # Rebalance
        for column in v_bucket_to_move:
            removed_items = self.nodes[column].remove_v_buckets(v_bucket_to_move[column])
            for item in removed_items:
                self.nodes[- 1].add_item(item)
