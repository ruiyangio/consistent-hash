import uuid
import cluster
from consistent import ConsistentHash
from hashprovider import get_hash_value
from scipy import stats

def test_uniformality(data, buckets, n_items):
    # Ktest with uniform cumulative distribution function
    d, p = stats.kstest(data, stats.uniform(loc=0.0, scale=(n_items / buckets)).cdf)
    if p < 0.01:
        return True
    return False

def test_get_hash():
    t = {}
    for i in range(100000):
        mo = get_hash_value(str(i)) % 5
        t[mo] = t[mo] + 1 if mo in t else 1

    if test_uniformality(list(t.values()), 5, 100000):
        print("uniform")
    else:
        print("broke")

def make_cluster(n_node, n_items, v_buckets):
    test_cluster = cluster.Cluster(n_node)
    consistent_hash = ConsistentHash("a", n_node, v_buckets)

    for i in range(n_items):
        # Make a random id
        item_id = uuid.uuid4().hex
        item = cluster.Item(item_id, str(i), get_hash_value(item_id))
        # find the bucket, bucket number directly map to node
        bucket = consistent_hash.get_column(item.id)
        test_cluster.nodes[bucket].add_item(item)
    
    # test uniformality
    item_dist = [ len(node.items) for node in test_cluster.nodes ]
    if test_uniformality(item_dist, n_node, n_items):
        print(item_dist)
        print("The cluster is made and items distribution is uniform")
    else:
        print("Cluster is not correctly made") 

    return (test_cluster, consistent_hash)

make_cluster(5, 10000, 1000)
