# -*- coding: UTF-8 -*-
from collections import Iterable


class Sortable(object):
    """
    sortable for redis
    """
    def __init__(self, database=None, key=None):
        self.database = database
        self.key = key

    def sort(self, by_pattern=None, start=None, num=None, get_pattern=None, is_desc=False,
             is_alpha=False, store=None):
        return self.database.sort(
            self.key, by=by_pattern, start=start, num=num, get=get_pattern,
            desc=is_desc, alpha=is_alpha, store=store,
        )


class Container(object):
    """
    基础空间
    """
    def __init__(self, database=None, key=None):
        self.database = database
        self.key = key

    def delete(self):
        """
        删除key
        :return:
        """
        self.database.delete(self.key)

    def expire(self, seconds=None, timestamp=None):
        """
        设置过期时间，如果什么都没有设置的话，则默认为永久保存
        :param seconds:
        :param timestamp:
        :return:
        """
        if seconds:
            self.database.expire(self.key, seconds)
        elif timestamp:
            self.database.expireat(self.key, timestamp)
        else:
            self.database.persist(self.key)

    def rename(self, new_key, check=True):
        """
        对key进行重命名
        :param new_key:
        :param check:
        :return:
        """
        if check:
            self.database.renamenx(self.key, new_key)
        else:
            self.database.rename(self.key, new_key)

    def left_seconds(self):
        """
        剩余的时间
        :return:
        """
        return self.database.ttl(self.key)


class Hash(Container):
    """
    哈希
    """
    def __repr__(self):
        return self.get_self()

    def clear(self):
        """
        清除所有的键值对，并且把自己给删除
        """
        self.delete()

    def _del_key(self, *args):
        self.database.hdel(self.key, args)

    def get(self, key, default=None):
        """
        获取值
        :param key:
        :param default:
        :return:
        """
        if key not in self:
            return default
        return self.database.hget(self.key, key)

    def __getitem__(self, key):
        """
        获取值
        :param key:
        :return:
        """
        return self.get(key)

    def has_key(self, key):
        """
        键是否存在
        :param key:
        :return:
        """
        return self.database.hexist(self.key, key)

    def __contains__(self, key):
        """
        键是否存在
        :param key:
        :return:
        """
        return self.database.hexist(self.key, key)

    def get_self(self):
        """
        获取字典内容
        :return:
        """
        return self.database.hgetall(self.key)

    def items(self):
        """
        获取所有键值对
        :return:
        """
        return self.database.hgetall(self.key)

    def _scan(self, cursor=0, pattern=None, count=None):
        """
        分片读取，避免一次获取大量的数据导致内存被挤爆
        :param cursor:
        :param pattern:
        :param count:
        :return:
        """
        return self.database.hscan_iter(self.key, cursor, pattern, count)

    def __iter__(self):
        """
        迭代器
        :return:
        """
        return iter(self._scan())

    def iteritems(self):
        """
        获取所有的键值对
        """
        return self._scan()

    def search(self, pattern, count=None):
        """
        搜索
        :param pattern:
        :param count:
        :return:
        """
        return self._scan(pattern=pattern, count=count)

    def keys(self):
        """
        获取所有的key
        :return:
        """
        return self.database.hkeys(self.key)

    def values(self):
        """
        获取所有的值
        """
        return self.database.hvals(self.key)

    def pop(self, key, default=None):
        """
        弹出指定的key
        :param key:
        :param default:
        :return:
        """
        if key not in self:
            if default:
                return default
            else:
                raise KeyError(u'not found the key')

        value = self.get(key)
        self._del_key([key])
        return value

    def popitem(self):
        """
        弹出任意的键值对
        :return:
        """
        if not self:
            raise KeyError(u'empty hash')

        rand_key, rand_val = next(self.iteritems())
        return rand_key, self.pop(rand_key)

    def __delitem__(self, key):
        self._del_key(key)

    def __setitem__(self, key, value):
        """
        设置键值对
        """
        self.database.hset(self.key, value)

    def setdefault(self, key, default=None):
        """
        设置默认值
        :param key:
        :param default:
        :return:
        """
        value = self.get(key)
        if not value:
            self[key] = default
        return value

    def update(self, other):
        """
        更新键值对
        :param other:
        :return:
        """
        if not other:
            return
        self.database.hmset(self.key, *(other.items()))

    def __eq__(self, other):
        """
        判断是否相等
        :param other:
        :return:
        """
        cur = self.get_self()
        return cur == other

    def __cmp__(self, other):
        """
        进行比较
        :param other:
        :return:
        """
        cur = self.get_self()
        return cmp(cur, other)

    def __len__(self):
        """
        获取长度
        :return:
        """
        return self.database.hlen(self.key)

    def __le__(self, other):
        """
        判断小于等于
        :param other:
        :return:
        """
        cur = self.get_self()
        return cur <= other

    def __ge__(self, other):
        """
        判断大于等于
        :param other:
        :return:
        """
        cur = self.get_self()
        return cur >= other

    def __ne__(self, other):
        """
        判断不等于
        :param other:
        :return:
        """
        cur = self.get_self()
        return cur != other


class Set(Sortable, Container):
    """
    集合
    """
    def add(self, val):
        """
        添加值
        :param val:
        :return:
        """
        self.database.sadd(self.key, val)

    def clear(self):
        """
        清除所有的键值对，并且把自己给删除
        """
        self.delete()

    def get_self(self):
        """
        获取集合内容
        """
        return self.database.smembers(self.key)

    def difference(self, other, with_score=False):
        """
        取差集
        :param other:
        :param with_score:
        :return:
        """
        if isinstance(other, Set):
            values = other.get_self()
        elif isinstance(other, set):
            values = other
        else:
            raise Exception('parameter format error')

        if with_score:
            return self.database.sdiffscore(self.key, values)
        return self.database.sdiff(self.key, values)

    def difference_store(self, dest_key, *args):
        """
        将差集的结果存储到新的key中
        :param dest_key:
        :param args:
        :return:
        """
        return self.database.sdiffstore(self.key, dest_key, args)

    def discard(self, val):
        """
        删除值
        :param val:
        :return:
        """
        self.database.srem(self.key, val)

    def intersection(self, other, with_score=False):
        """
        取交集
        :param other:
        :param with_score:
        :return:
        """
        if isinstance(other, Set):
            values = other.get_self()
        elif isinstance(other, set):
            values = other
        else:
            raise Exception('parameter format error')

        if with_score:
            return self.database.sinterscore(self.key, values)
        return self.database.sinter(self.key, values)

    def intersection_store(self, dest_key, *args):
        """
        将交集的结果存储到新的key中
        :param dest_key:
        :param args:
        :return:
        """
        return self.database.sinterstore(self.key, dest_key, args)

    def _scan(self, pattern=None, count=None):
        """
        分片读取，避免一次获取大量的数据导致内存被挤爆
        :param pattern:
        :param count:
        :return:
        """
        return self.database.zscan_iter(self.key, pattern, count)

    def issubset(self, other):
        """
        判断是否是子集
        :param other:
        :return:
        """
        cur = self.get_self()
        return cur.issubset(other)

    def issuperset(self, other):
        """
        判断是否是父集
        :param other:
        :return:
        """
        cur = self.get_self()
        return cur.issuperset(other)

    def pop(self, count=1):
        """
        随机弹出值
        :param count:
        :return:
        """
        return self.database.spop(self.key, count)

    def remove(self, *args):
        """
        删除元素
        :param args:
        :return:
        """
        self.database.srem(self.key, args)

    def union(self, other, with_score=False):
        """
        取并集
        :param other:
        :param with_score:
        :return:
        """
        if isinstance(other, Set):
            values = other.get_self()
        elif isinstance(other, set):
            values = other
        else:
            raise Exception('parameter format error')

        if with_score:
            return self.database.sunionstore(self.key, values)
        return self.database.sunion(self.key, values)

    def union_store(self, dest_key, *args):
        """
        将并集的结果存储到新的key中
        :param dest_key:
        :param args:
        :return:
        """
        return self.database.sunionstore(self.key, dest_key, args)

    def update(self, other):
        """
        并集
        :param other:
        :return:
        """
        return self.union(other)

    def rand(self, count=None):
        """
        返回指定个数的值
        :param count:
        :return:
        """
        return self.database.srandmember(self.key, count)

    def __contains__(self, item):
        """
        判断元素是否存在
        :param item:
        :return:
        """
        return self.database.sismember(self.key, item)

    def __iand__(self, other):
        """
        cur &= other
        """
        return self.intersection(other)

    def __ior__(self, other):
        """
        cur |= other
        """
        return self.union(other)

    def __isub__(self, other):
        """
        cur -= other
        """
        return self.difference(other)

    def __iter__(self):
        """
        迭代器
        :return:
        """
        return iter(self._scan())

    def __ixor__(self, other):
        """
        cur ^= other
        """
        pass

    def __len__(self):
        """
        获取长度
        """
        return self.database.scard(self.key)


class ZSet(Sortable, Container):
    def get_self(self, is_with_score=False):
        """
        返回有序集合的结果
        :param is_with_score:
        :return:
        """
        return self.range(0, -1, False, is_with_score)

    def append(self, mapping=None, **kwargs):
        """
        添加成员分值，成员到有序集合中
        :param mapping:
        :param kwargs:
        :return:
        """
        if not mapping:
            _mapping = mapping.copy()
            _mapping.update(kwargs)
        else:
            _mapping = mapping
        self.database.zadd(self.key, _mapping)

    def incr(self, member, amount=1.):
        """
        增加指定值
        :param member:
        :param amount:
        :return:
        """
        self.database.zincrby(self.key, amount, member)

    def intersection_store(self, dest_key, *args):
        """
        将交集的结果存储到新的key中
        :param dest_key:
        :param args:
        :return:
        """
        return self.database.zinterstore(self.key, dest_key, args)

    def union_store(self, dest_key, *args):
        """
        将并集的结果存储到新的key中
        :param dest_key:
        :param args:
        :return:
        """
        return self.database.zunionstore(self.key, dest_key, args)

    def pop_max(self, count=1):
        """
        弹出指定个数的最大值（根据分值排序）
        :param count:
        :return:
        """
        return self.database.zpopmax(self.key, count)

    def pop_min(self, count=1):
        """
        弹出指定个数的最小值（根据分值排序）
        :param count:
        :return:
        """
        return self.database.zpopmin(self.key, count)

    def __getitem__(self, item):
        """
        如果是切分则切分，否则返回具体排名的分值
        :param item:
        :return:
        """
        if isinstance(item, slice):
            start = item.start or 0
            stop = item.stop or len(self)
            step = item.step or 1
            if step > 0:
                return self.range(start, stop)
            else:
                return self.range(start, stop, True)
        elif isinstance(item, int):
            return self.database.zscore(self.key, item)
        else:
            raise Exception(u'parameter format error')

    def __setitem__(self, item, value):
        """
        设置值
        :param key:
        :param value:
        :return:
        """
        self.append({item: value})

    def __delitem__(self, item):
        """
        删除成员
        :param item:
        :return:
        """
        self.remove([item])

    def __delslice__(self, min, max):
        """
        删除成员
        :param min:
        :param max:
        :return:
        """
        self.remove_by_rank(min, max)

    def range(self, start, stop, is_reverse=False, is_desc=False, is_with_scores=False):
        """
        获取指定的范围
        :param start:
        :param stop:
        :param is_reverse:
        :param is_desc:
        :param is_with_scores:
        :return:
        """
        if not is_reverse:
            return self.database.zrange(self.key, start, stop, is_desc, is_with_scores)
        return self.database.zrevrange(self.key, start, stop, is_with_scores)

    def range_by_score(self, min, max, start=None, stop=None, is_reverse=False, is_with_scores=False):
        """
        根据分值来取
        :param min:
        :param max:
        :param start:
        :param stop:
        :param is_reverse:
        :param is_with_scores:
        :return:
        """
        num = None
        if stop and start:
            num = stop - start + 1
        if not is_reverse:
            return self.database.zrangebyscore(min, max, start, num, is_with_scores)
        return self.database.zrevrangebyscore(min, max, start, num, is_with_scores)

    def rank(self, value, is_reverse=False):
        """
        获取排名
        :param value:
        :param is_reverse:
        :return:
        """
        if not is_reverse:
            return self.database.zrank(self.key, value)
        return self.database.zrevrank(self.key, value)

    def score(self, value):
        """
        获取分值
        :param value:
        :return:
        """
        return self.database.zscore(self.key, value)

    def remove(self, *members):
        """
        删除成员
        :param members:
        :return:
        """
        self.database.zrem(self.key, members)

    def remove_by_rank(self, min, max):
        """
        通过排名来删除
        :param min:
        :param max:
        :return:
        """
        self.database.zremrangebyrank(self.key, min, max)

    def remove_by_score(self, min, max):
        """
        通过分值来删除
        :param min:
        :param max:
        :return:
        """
        self.database.zremrangebyscore(self.key, min, max)

    def __repr__(self):
        return self.get_self()

    def __reversed__(self):
        """
        反转
        :return:
        """
        return self.range(0, -1, True)

    def __iadd__(self, other):
        """
        累加
        :param other:
        :return:
        """
        if isinstance(other, ZSet):
            self.append(dict(other.get_self(True)))
        elif isinstance(other, (list, tuple)):
            self.append(dict(other))
        elif isinstance(other, dict):
            self.append(other)
        else:
            raise Exception(u'parameter format error')

    def __contains__(self, item):
        """
        判断元素是否在有序集合中
        """
        return bool(self.score(item))

    def __iter__(self):
        """
        iter for set
        """
        return iter(self.get_self())

    def __len__(self):
        """
        获取长度
        """
        return self.database.zcard(self.key)


class List(Sortable, Container):
    def get_self(self):
        """
        获取列表内容
        :return:
        """
        return self.database.lrange(self.key, 0, -1)

    def append(self, val):
        """
        往数组末尾添加值
        :param val:
        :return:
        """
        self.database.rpush(self.key, val)

    def count(self, val):
        """
        获取值在列表中出现的次数
        :param val:
        :return:
        """
        cur = self.get_self()
        return cur.count(val)

    def extend(self, values):
        """
        添加数组
        :param values:
        :return:
        """
        self.database.rpush(self.key, values)

    def insert(self, index, value):
        """
        往指定位置插入值
        :param index:
        :param value:
        :return:
        """
        self.database.linsert(self.key, index, 'before', value)

    def pop(self, index=None):
        """
        弹出指定位置的值，默认弹出末尾的值
        :param index:
        :return:
        """
        if not self:
            raise Exception("empty list")

        if index is None:
            return self.database.rpop(self.key)
        else:
            l = len(self)
            if not (0 <= index < l):
                raise Exception('index is out of range')

            val = self[index]
            self.database.lset(self.key, index, "__del")
            self.database.lrem(self.key, 1, "__del")
            return val

    def remove(self, value, count=1):
        """
        删除指定个数的出现的值
        :param value:
        :param count:
        :return:
        """
        if value not in self:
            raise Exception("not found in the list")
        self.database.lrem(self.key, count, value)

    def __delitem__(self, index):
        """
        删除指定下标的值
        :param index:
        :return:
        """
        self.pop(index)

    def __delslice__(self, start, end):
        """
        删除区间
        :param i:
        :param j:
        :return:
        """
        self.database.ltrim(self.key, start, end)

    def __getitem__(self, item):
        """
        cur[index]
        """
        if isinstance(item, slice):
            start = item.start or 0
            stop = item.stop or len(self)
            return self[start, stop]

        return self[item]

    def __getslice__(self, start, end):
        """
        获取区间的值
        :param start:
        :param end:
        :return:
        """
        return self.database.lrange(self.key, start, end)

    def __iadd__(self, other):
        """
        累加
        :param other:
        :return:
        """
        if isinstance(other, List):
            values = other.get_self()
        elif isinstance(other, Iterable):
            values = other
        else:
            raise Exception('parameter format error')

        self.extend(values)

    def __iter__(self):
        """
        iter for list
        """
        return iter(self.get_self())

    def __len__(self):
        """
        获取列表长度
        :return:
        """
        return self.database.llen(self.key)

    def __repr__(self):
        return self.get_self()

    def __setitem__(self, index, val):
        """
        设置值
        :param index:
        :param val:
        :return:
        """
        l = len(self)
        if not (0 <= index < l):
            raise Exception('index is out of range')
        return self.database.lset(self.key, index, val)
