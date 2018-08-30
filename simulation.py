import uuid
import random
import cluster
import util
from consistent import ConsistentHash
from hashprovider import get_hash_value

def make_cluster(n_node, n_items, v_buckets):
    test_cluster = cluster.Cluster(n_node, v_buckets)
    test_cluster.generate_items(n_items)
    return test_cluster

def simulation(steps, n_node, n_items, v_buckets):
    print("Create cluster: ")
    test_cluster = make_cluster(n_node, n_items, v_buckets)
    operations = ["add_node", "delete_node", "add_items", "get_items"]

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
        elif rand_operation == "add_items":
            n_items = random.randint(1000, 20000)
            print("-------------------------------------")
            print("Add items: " + str(n_items))
            test_cluster.generate_items(n_items)
        elif rand_operation == "get_items":
            ids = test_cluster.get_all_item_ids()
            rand_ids = random.sample(ids, int(len(ids) * 0.3))
            print("-------------------------------------")
            print("Try get some amount of items: " + str(len(rand_ids)))
            for item_id in rand_ids:
                item = test_cluster.get_item(item_id)
                if item.id != item_id:
                    raise ValueError('Expect: {} but got {}'.format(item_id, item.id))

simulation(10, 5, 100000, 10000)
