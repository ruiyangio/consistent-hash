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
    test_cluster.report_uniformality()

    return test_cluster

# Init cluster with 5 node, 10000 items and 1000 virtual buckets
test_cluster = make_cluster(5, 10000, 1000)
# Add node
test_cluster.add_node_and_rebalance()
# Add 10000 more new items
test_cluster.generate_items(10000)
# Add 9000 more new items
test_cluster.generate_items(9000)
# Add one more node
test_cluster.add_node_and_rebalance()

# Remove first node
first_node_id = list(test_cluster.nodes.keys())[0]
test_cluster.remove_node_and_rebalance(first_node_id)

# Add one more node
test_cluster.add_node_and_rebalance()
