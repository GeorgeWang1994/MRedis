# -*- coding: UTF-8 -*-

from test_basic import TestBasic


class TestHash(TestBasic):
    """
    测试哈希
    """
    def setUp(self):
        super(TestHash, self).setUp()
        self.hash = self.gredis.Hash('test_hash')

    def test_get_set(self):
        """
        测试获取和设置
        """
        self.assertEqual(len(self.hash), 0)
        self.hash.setdefault('first', 1)
        self.hash['second'] = 2
        self.hash.update({"third": 3, "forth": 4})

        self.assertEqual(len(self.hash), 4)
        keys = ["first", "second", "third", "forth"]
        for idx in range(4):
            key = keys[idx]
            self.assertEqual(self.hash[key], str(idx + 1))
            self.assertEqual(self.hash.get(key), str(idx + 1))
            self.assertTrue(self.hash.has_key(key))
            self.assertTrue(key in self.hash)

        self.assertEqual(self.hash.get("six", -1), -1)

    def test_remove(self):
        """
        测试删除
        """
        self.assertEqual(len(self.hash), 0)
        self.hash.update({"first": 1, "second": 2, "third": 3, "forth": 4})

        del self.hash['first']
        self.assertEqual(len(self.hash), 3)
        self.assertTrue('first' not in self.hash)

        self.assertEqual(self.hash.pop("second", -1), '2')
        self.assertEqual(len(self.hash), 2)
        self.assertTrue('second' not in self.hash)

        self.assertTrue(self.hash.popitem())
        self.assertEqual(len(self.hash), 1)

        self.hash.clear()
        self.assertEqual(len(self.hash), 0)

    def test_iter(self):
        """
        测试遍历
        """
        self.hash.update({"first": 1, "second": 2, "third": 3, "forth": 4})
        self.assertEqual(sorted(self.hash.items(), key=lambda x: x[1]),
                         [('first', '1'), ('second', '2'), ('third', '3'), ('forth', '4')])

        keys = ['first', 'second', 'third', 'forth']
        self.assertSetEqual(set(self.hash.keys()), set(keys))
        self.assertSetEqual(set(self.hash.values()), {'1', '2', '3', '4'})

        for key, val in self.hash:
            self.assertTrue(key in keys)
            self.assertEqual(self.hash[key], val)

    def test_incr_desc(self):
        """
        测试增加减少
        """
        self.hash['first'] = 1
        self.hash.incr('first', 2)
        self.assertEqual(self.hash['first'], '3')
        self.hash.desc('first', 3)
        self.assertEqual(self.hash['first'], '0')

        self.hash['first'] = 1.1
        self.hash.incr_float('first', 2.2)
        self.assertEqual(self.hash['first'], '3.3')
        # 当计算的结果得到是整数的时候，那么取出来的结果就是整数，而不是3.0的浮点数
        self.hash.desc_float('first', 0.3)
        self.assertEqual(self.hash['first'], '3')


class TestSet(TestBasic):
    """
    测试集合
    """
    def setUp(self):
        super(TestSet, self).setUp()
        self.set1 = self.gredis.Set('test_set1')
        self.set2 = self.gredis.Set('test_set2')
        self.set3 = self.gredis.Set('test_set3')

    def test_add_get_pop(self):
        """
        测试增加减少
        """
        self.set1.add(1)
        self.assertEqual(len(self.set1), 1)

        self.set1.discard(1)
        self.assertEqual(len(self.set1), 0)

        self.set1.update({2, 3, 4})
        self.set1.remove(2, 3)
        self.assertEqual(len(self.set1), 1)

        result = self.set1.rand(1)
        self.assertSetEqual(set(result), {'4'})

        self.set1.pop(2)
        self.assertEqual(len(self.set1), 0)

        self.set1.clear()
        self.assertEqual(len(self.set1), 0)

    def test_union_inter_difference(self):
        """
        测试集合相关功能
        """
        self.set1.update({1, 2, 3})
        self.assertEqual(len(self.set1), 3)

        self.set2.update({2, 3, 4})
        self.assertEqual(len(self.set2), 3)

        self.set3.update({4, 5, 6})
        self.assertEqual(len(self.set3), 3)

        self.set1.update(self.set2)
        self.assertEqual(len(self.set1), 4)

        self.set1.union_store(self.set1.cache_key, self.set2, self.set3)
        self.assertEqual(len(self.set1), 6)

        self.set1.intersection_store(self.set1.cache_key, self.set2)
        self.assertEqual(len(self.set1), 3)
        self.assertSetEqual(self.set1.get_self(), {'2', '3', '4'})

        self.set1.difference_store(self.set1.cache_key, self.set3)
        self.assertEqual(len(self.set1), 2)
        self.assertSetEqual(self.set1.get_self(), {'2', '3'})

    def test_iter(self):
        """
        测试遍历
        """
        values = [1, 2, 3, 4]
        self.set1.update(values)
        for val in self.set1:
            self.assertTrue(int(val) in values)
