import uuid
import random
import util
from scipy import stats
from functools import reduce
from hashprovider import get_hash_value
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

    def get_item(self, item_id):
        if item_id in self.items:
            return self.items[item_id]
    
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
            self.v_bucket_map.pop(v_bucket_key)
        return removed_items

class Cluster(object):
    def __init__(self, n=1, v_buckets=1000):
        self.n = n
        self.nodes = {}
        self.meta = ConsistentHash("a", n, v_buckets)

        for column_id in self.meta.column_ids:
            self.nodes[column_id] = Node(column_id)

    def _make_hash_key(self, item_id):
        return get_hash_value(item_id)

    def _get_item_debug(self, item_id):
        for node_id in self.nodes:
            item = self.nodes[node_id].get_item(item_id)
            if item:
                return (node_id, item)

    def get_all_item_ids(self):
        return [ item_id for node in self.nodes.values() for item_id in node.items.keys() ]

    def get_item_dist(self):
        return [ len(node.items) for node in self.nodes.values() ]

    def get_item_count(self):
        return reduce((lambda a, b: a + b), self.get_item_dist())
    
    def get_item(self, item_id):
        column_id, v_bucket_key = self.meta.get_column_with_hash_key(self._make_hash_key(item_id))
        return (self.nodes[column_id].get_item(item_id), column_id)
    
    def insert_item(self, item):
        column_id, v_bucket_key = self.meta.get_column_with_hash_key(item.hash_key)
        item.v_bucket_key = v_bucket_key
        self.nodes[column_id].add_item(item)

    def remove_item(self, item_id):
        item, node_id = self.get_item(item_id)
        self.nodes[node_id].remove_item(item.id)

    def generate_items(self, n, do_report=True):
        for i in range(n):
            item_id = uuid.uuid4().hex
            item = Item(item_id, str(i), self._make_hash_key(item_id))
            self.insert_item(item)
        
        return self.report_uniformality(do_report)

    def add_node_and_rebalance(self, do_report=True):
        self.n += 1
        new_column_id, v_bucket_to_move = self.meta.add_column()
        self.nodes[new_column_id] = Node(new_column_id)
        # Rebalance
        for column_id in v_bucket_to_move:
            removed_items = self.nodes[column_id].remove_v_buckets(v_bucket_to_move[column_id])
            for item in removed_items:
                self.nodes[new_column_id].add_item(item)
        # Report uniformality
        return self.report_uniformality(do_report)
    
    def remove_node_and_rebalance(self, node_id, do_report=True):
        self.n -= 1
        v_bucket_to_move = self.meta.remove_column(node_id)

        # Rebalance
        for column_id in v_bucket_to_move:
            removed_items = self.nodes[node_id].remove_v_buckets(v_bucket_to_move[column_id])
            for item in removed_items:
                self.nodes[column_id].add_item(item)

        # Remove node
        self.nodes.pop(node_id)
        return self.report_uniformality(do_report)

    def report_uniformality(self, do_report=True):
        if not do_report:
            return
        is_uniform = False
        item_dist = self.get_item_dist()
        coeffient_of_variation = stats.variation(item_dist)
        total_count = self.get_item_count()
        
        print("-------------------------------------")
        print("total: " + str(total_count))
        print("coeffient_of_variation: " + str(coeffient_of_variation))
        print(item_dist)
        
        if util.test_uniformality(item_dist, len(self.nodes), total_count):
            is_uniform = True
            print("Items distribution is uniform")
        else:
            print("Not uniform")
        
        return (is_uniform, coeffient_of_variation)
