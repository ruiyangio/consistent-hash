import uuid
import cluster
import util
from consistent import ConsistentHash
from hashprovider import get_hash_value

def make_cluster(n_node, n_items, v_buckets):
    test_cluster = cluster.Cluster(n_node)
    consistent_hash = ConsistentHash("a", n_node, v_buckets)

    for i in range(n_items):
        # Make a random id
        item_id = uuid.uuid4().hex
        item = cluster.Item(item_id, str(i), get_hash_value(item_id, item_id[0], item_id[-1]))
        # find the bucket, bucket number directly map to node
        bucket, v_bucket_key = consistent_hash.get_column_with_hash_key(item.hash_key)
        item.v_bucket_key = v_bucket_key
        test_cluster.nodes[bucket].add_item(item)
    
    # test uniformality
    item_dist = [ len(node.items) for node in test_cluster.nodes ]
    if util.test_uniformality(item_dist, n_node, n_items):
        print(item_dist)
        print("The cluster is made and items distribution is uniform")
    else:
        print("Cluster is not correctly made") 

    return (test_cluster, consistent_hash)

test_cluster, hash_meta = make_cluster(5, 10000, 1000)
# Add node
test_cluster.add_node_and_rebalance(hash_meta)
print(test_cluster.get_item_dist())

for i in range(10000):
    item_id = uuid.uuid4().hex
    item = cluster.Item(item_id, str(i), get_hash_value(item_id, item_id[0], item_id[-1]))
    bucket, v_bucket_key = hash_meta.get_column_with_hash_key(item.hash_key)
    item.v_bucket_key = v_bucket_key
    test_cluster.nodes[bucket].add_item(item)

print(test_cluster.get_item_dist())
