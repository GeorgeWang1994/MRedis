# -*- coding: UTF-8 -*-
from collections import Mapping


class Counter(object):
    """
    计数器
    """
    def __init__(self, database, cache_key, expire_time=60 * 60 * 24):
        self.database = database
        self.cache_key = cache_key
        self.expire_time = expire_time

    def incr(self, key, amount=1):
        """
        增加指定的值，每次更新后会重新设置过期时间
        :param key:
        :param amount:
        :return:
        """
        count = self.database.hincrby(self.cache_key, key, amount)
        if not count:
            self.remove(key)
        self._expire()
        return count

    def decr(self, key, amount=-1):
        """
        减少指定的值
        :param key:
        :param amount:
        :return:
        """
        return self.incr(key, amount)

    def value(self, key):
        """
        当前的值
        :param key:
        :return:
        """
        return self.database.hget(self.cache_key, key) or 0

    def values(self, keys):
        """
        批量获取
        :param keys:
        :return:
        """
        if not keys:
            return []

        return self.database.hmget(self.cache_key, keys)

    def _expire(self):
        """
        设置过期时间
        :return:
        """
        self.database.expire(self.cache_key, self.expire_time)

    def update(self, other):
        """
        添加值，可以是可迭代对象
        :param other:
        :return:
        """
        if not other:
            raise TypeError('parameter can not be empty.')

        if isinstance(other, Mapping):
            pass
        elif isinstance(other, Counter):
            other = other.as_dict()
        else:
            raise TypeError('parameter format error')

        self.database.hmset(self.cache_key, other)

    def remove(self, *args):
        """
        删除键
        :param args:
        :return:
        """
        self.database.hdel(self.cache_key, args)

    def clear(self):
        """
        清除所有的数字
        :return:
        """
        self.database.delete(self.cache_key)

    def __setitem__(self, key, value):
        """
        设置键值对
        :param key:
        :param value:
        :return:
        """
        self.database.hset(self.cache_key, key, value)
        self._expire()

    def __delitem__(self, key):
        """
        删除键
        :param key:
        :return:
        """
        self.remove(key)

    def as_dict(self):
        """
        获取所有键值对
        :return:
        """
        return self.database.hgetall(self.cache_key)
