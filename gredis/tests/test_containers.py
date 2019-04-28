# -*- coding: UTF-8 -*-

from .test_basic import TestBasic


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


class TestSortedSet(TestBasic):
    """
    测试有序集合
    """
    def setUp(self):
        super(TestSortedSet, self).setUp()
        self.sorted_set = self.gredis.SortedSet('test_sorted_set')

    def test_append_remove(self):
        """
        测试添加删除
        """
        self.sorted_set.append({'third': 3, 'second': 2, 'first': 1})
        self.sorted_set.append(fifth=5, forth=4)

        result = self.sorted_set.pop_max(1)
        self.assertEqual(result[0], (b'fifth', 5.0))
        self.assertEqual(len(self.sorted_set), 4)

        result = self.sorted_set.pop_min(1)
        self.assertEqual(result[0], (b'first', 1.0))
        self.assertEqual(len(self.sorted_set), 3)

        del self.sorted_set['second']
        self.assertEqual(len(self.sorted_set), 2)

        self.sorted_set.remove('third')
        self.assertEqual(len(self.sorted_set), 1)

        del self.sorted_set[:1]
        self.assertEqual(len(self.sorted_set), 0)

        self.sorted_set.append({'third': 3, 'second': 2, 'first': 1})
        self.assertEqual(len(self.sorted_set), 3)

        # 删除两个，分别是first和second
        self.sorted_set.remove_by_rank(0, 1)
        self.assertEqual(len(self.sorted_set), 1)
        # 没有删除
        self.sorted_set.remove_by_score(0, 1)
        self.assertEqual(len(self.sorted_set), 1)
        self.assertTrue('third' in self.sorted_set)
        # 删除third
        self.sorted_set.remove_by_score(2, 3)
        self.assertEqual(len(self.sorted_set), 0)

    def test_get_set(self):
        """
        测试获取设置
        """
        self.sorted_set['first'] = 1
        self.sorted_set.append({'third': 3, 'second': 2})
        # 测试range
        result = self.sorted_set.range(0, 1)
        self.assertEqual(result, [b'first', b'second'])
        result = self.sorted_set.range(0, 1, is_desc=True)
        self.assertEqual(result, [b'third', b'second'])
        result = self.sorted_set.range(0, 1, is_reverse=True)
        self.assertEqual(result, [b'third', b'second'])
        result = self.sorted_set.range(0, 1, is_with_scores=True)
        self.assertEqual(result, [(b'first', 1), (b'second', 2)])

        # 测试range_by_score
        result = self.sorted_set.range_by_score(1, 2)
        self.assertEqual(result, [b'first', b'second'])
        result = self.sorted_set.range_by_score(1, 2, 0, 1)
        self.assertEqual(result, [b'first', b'second'])
        result = self.sorted_set.range_by_score(1, 2, is_reverse=True)
        self.assertEqual(result, [b'second', b'first'])
        result = self.sorted_set.range_by_score(1, 2, is_with_scores=True)
        self.assertEqual(result, [(b'first', 1.0), (b'second', 2.0)])

        # 测试rank和score
        result = self.sorted_set.score('first')
        self.assertEqual(result, 1)
        result = self.sorted_set.rank('first')
        self.assertEqual(result, 0)

    def test_iter(self):
        """
        测试遍历
        """
        self.sorted_set.append({'third': 3, 'second': 2, 'first': 1})
        for member, score in self.sorted_set:
            self.assertTrue(member)
            self.assertTrue(score)


class TestList(TestBasic):
    """
    测试列表
    """
    def setUp(self):
        super(TestList, self).setUp()
        self.list = self.gredis.List('test_list')

    def test_append_remove(self):
        """
        测试添加删除
        """
        self.list.append(10)
        self.list.extend([20, 30, 40])
        self.list.prepend(0)
        self.assertEqual(len(self.list), 5)
        self.list.insert_by_value(10, 1)
        self.assertEqual(len(self.list), 6)
        self.list.pop(0)
        self.assertEqual(len(self.list), 5)
        self.list.remove(1)
        self.assertEqual(len(self.list), 4)
        self.list[0] = 1
        self.assertEqual(len(self.list), 4)
        del self.list[0]
        self.assertEqual(len(self.list), 3)
        self.list.trim(0, 0)
        self.assertEqual(len(self.list), 1)
        self.list += [1, 2, 3]
        self.assertEqual(len(self.list), 4)
        result = self.list[1: 3]
        self.assertEqual(result, [b'1', b'2', b'3'])
        result = self.list[-1]
        self.assertEqual(result, b'3')
