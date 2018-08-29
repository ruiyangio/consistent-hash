import uuid
import random
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

def simulation(steps, n_node, n_items, v_buckets):
    print("Create cluster: ")
    test_cluster = make_cluster(n_node, n_items, v_buckets)
    operations = ["add_node", "delete_node", "add_items"]

    for i in range(steps):
        rand_operation = random.choice(operations)

        if rand_operation == "add_node":
            print("-------------------------------------")
            print("Add one node to cluster")
            test_cluster.add_node_and_rebalance()
        elif rand_operation == "delete_node":
            node_ids = list(test_cluster.nodes.keys())
            # only remove if there is node
            if len(node_ids) > 1:
                rand_node_id = random.choice(node_ids)
                print("-------------------------------------")
                print("Remove node: " + str(rand_node_id))
                test_cluster.remove_node_and_rebalance(rand_node_id)
        else:
            n_items = random.randint(1000, 20000)
            print("-------------------------------------")
            print("Add items: " + str(n_items))
            test_cluster.generate_items(n_items)

simulation(10, 5, 1000000, 500000)
