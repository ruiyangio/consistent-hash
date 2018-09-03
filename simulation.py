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

def print_seperator():
    print("-------------------------------------")

def scale_out(test_cluster, n_items, depth):
    for i in range(depth):
        test_cluster.generate_items(n_items, do_report=False)
        test_cluster.add_node_and_rebalance(do_report=False)
    test_cluster.report_uniformality()

def process_report(report, coefficients):
    uniform_count = 0
    if report:
        if report[0]:
            uniform_count = 1
        coefficients.append(report[1])
    return uniform_count

def simulation(steps, n_node, n_items, v_buckets, scale_out_times=5):
    print("Creating cluster...")
    test_cluster = make_cluster(n_node, n_items, v_buckets)
    print("Cluster is created")
    operations = ["add_node", "delete_node", "add_items", "get_items", "remove_items"]

    print("Scale out {:d} times".format(scale_out_times))
    scale_out(test_cluster, n_items, scale_out_times)
    print_seperator()

    uniform_count = 0
    coefficients = []

    for i in range(steps):
        rand_operation = random.choice(operations)

        if rand_operation == "add_node":
            print_seperator()
            print("Add one node to cluster")
            report = test_cluster.add_node_and_rebalance()
            uniform_count += process_report(report, coefficients)
        elif rand_operation == "delete_node":
            node_ids = list(test_cluster.nodes.keys())
            # only remove if there is node
            if len(node_ids) > 1:
                rand_node_id = random.choice(node_ids)
                print_seperator()
                print("Remove node: " + str(rand_node_id))
                report = test_cluster.remove_node_and_rebalance(rand_node_id)
                uniform_count += process_report(report, coefficients)
        elif rand_operation == "add_items":
            n_items = random.randint(1000, 20000)
            print_seperator()
            print("Add items: " + str(n_items))
            report = test_cluster.generate_items(n_items)
            uniform_count += process_report(report, coefficients)
        elif rand_operation == "get_items":
            ids = test_cluster.get_all_item_ids()
            rand_ids = random.sample(ids, int(len(ids) * 0.3))
            print_seperator()
            print("Try get some amount of items: " + str(len(rand_ids)))
            for item_id in rand_ids:
                item, node_id = test_cluster.get_item(item_id)
                if item.id != item_id:
                    raise ValueError('Expect: {} but got {}'.format(item_id, item.id))
            uniform_count += 1
        elif rand_operation == "remove_items":
            ids = test_cluster.get_all_item_ids()
            rand_ids = random.sample(ids, int(len(ids) * 0.15))
            print_seperator()
            print("Remove items: " + str(len(rand_ids)))
            for item_id in rand_ids:
                test_cluster.remove_item(item_id)
            report = test_cluster.report_uniformality()
            uniform_count += process_report(report, coefficients)

    print_seperator()
    print("Total number of operations: {} Total number of operations result in even distribution: {:d}".format(steps, uniform_count))
    print("Max coefficient of variation: {:f}".format(max(coefficients)))

simulation(100, 5, 100000, 500000)
