import unittest

from pyalink.alink.common.types.bases.params import Params


class TestParams(unittest.TestCase):
    def test_set(self):
        x = Params()
        x.set('key1', 'val1')
        x.set('key2', 'val2')
        self.assertTrue('key1' in x)
        self.assertTrue('key2' in x)
        self.assertTrue('key3' not in x)

    def test_get(self):
        x = Params()
        x['key1'] = 1
        x['key2'] = 2
        self.assertEqual(x['key1'], 1)
        self.assertEqual(x.get('k', 5), 5)
        del x['key2']
        self.assertEqual(x.get('key2', 5), 5)

    def test_from_args(self):
        g = Params.from_args(Params().set('a', 1), b=3, c=5)
        self.assertTrue('a' in g)
        self.assertTrue('b' in g)
        self.assertTrue('c' in g)
        self.assertTrue('d' not in g)
        self.assertEqual(g['a'], 1)
        self.assertEqual(g['b'], 3)
        self.assertEqual(g['c'], 5)
