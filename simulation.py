import uuid
import cluster
import util
from consistent import ConsistentHash
from hashprovider import get_hash_value

def make_cluster(n_node, n_items, v_buckets):
    test_cluster = cluster.Cluster(n_node, v_buckets)

    for i in range(n_items):
        # Make a random id
        item_id = uuid.uuid4().hex
        item = cluster.Item(item_id, str(i), get_hash_value(item_id, item_id[0], item_id[-1]))
        # find the bucket, bucket number directly map to node
        test_cluster.insert_item(item)
    
    # test uniformality
    item_dist = test_cluster.get_item_dist()
    if util.test_uniformality(item_dist, n_node, n_items):
        print(item_dist)
        print("The cluster is made and items distribution is uniform")
    else:
        print("Cluster is not correctly made") 

    return test_cluster

test_cluster = make_cluster(5, 10000, 1000)
# Add node
test_cluster.add_node_and_rebalance()
print(test_cluster.get_item_dist())

for i in range(10000):
    item_id = uuid.uuid4().hex
    item = cluster.Item(item_id, str(i), get_hash_value(item_id, item_id[0], item_id[-1]))
    test_cluster.insert_item(item)
print(test_cluster.get_item_dist())

first_node_id = list(test_cluster.nodes.keys())[0]
test_cluster.remove_node_and_rebalance(first_node_id)
print(test_cluster.get_item_dist())
