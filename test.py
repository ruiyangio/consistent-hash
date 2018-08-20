from cluster import Cluster, Item
from consistent import ConsistentHash
from uuid import uuid4

def build_test_cluster(n_nodes, n_item):
    test_cluster = Cluster(n_nodes)
    consistent = ConsistentHash("a", n_nodes)

    for i in range(n_item):
        item = Item(uuid4().hex, i)
        col = consistent.get_column(str(item.id))
        test_cluster.nodes[col].add_item(item)
    
    return test_cluster, consistent

test_cluster, consistent = build_test_cluster(5, 500)

print(len(test_cluster.nodes))

for node in test_cluster.nodes:
    print(len(node.items))

print(len(consistent.circle.keys()))
