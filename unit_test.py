import unittest
import hashprovider
import util
from consistent import ConsistentHash

class TestConsistentHash(unittest.TestCase):

    def test_find_first_ge(self):
        a = [1, 2, 5, 7, 9, 10]
        b = [1]

        self.assertEqual(util.find_first_ge(a, 0), 1)
        self.assertEqual(util.find_first_ge(a, 1), 1)
        self.assertEqual(util.find_first_ge(a, 2), 2)
        self.assertEqual(util.find_first_ge(a, 5), 5)
        self.assertEqual(util.find_first_ge(a, 8), 9)
        self.assertEqual(util.find_first_ge(a, 11), -1)
        self.assertEqual(util.find_first_ge(b, 0), 1)
        self.assertEqual(util.find_first_ge(b, 2), -1)

    def test_get_hash_value(self):
        t = {}
        for i in range(100000):
            mo = hashprovider.get_hash_value(str(i), "a", "b") % 5
            t[mo] = t[mo] + 1 if mo in t else 1

        self.assertTrue(util.test_uniformality(list(t.values()), 5, 100000))

if __name__ == '__main__':
    unittest.main()