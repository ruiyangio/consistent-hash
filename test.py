from cluster import Cluster

test_cluster = Cluster(2)

for node in test_cluster.nodes:
    print(node.id)

test_cluster.add_node()

for node in test_cluster.nodes:
    print(node.id)

test_cluster.add_node()

for node in test_cluster.nodes:
    print(node.id)