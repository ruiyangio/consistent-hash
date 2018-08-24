class Item(object):
    def __init__(self, id, name, hash_key):
        self.id = id
        self.name = name
        self.hash_key = hash_key

class Node(object):
    def __init__(self, id):
        self.id = id
        self.items = {}

    def add_item(self, item):
        self.items[item.id] = item

class Cluster(object):
    def __init__(self, n=1):
        self.nodes = []
        self.n = n

        for i in range(self.n):
            self.nodes.append(Node(i))

    def add_node(self):
        self.nodes.append(Node(self.n))
        self.n += 1
