# -*- coding: UTF-8 -*-

from redis import Redis

from .containers import List, Set, ZSet, Hash, HyperLogLog
from .counter import Counter
from .rate_limit import RateLimit


class GRedis(Redis):
    """
    redis客户端
    """
    def __init__(self, *args, **kwargs):
        super(GRedis, self).__init__(*args, **kwargs)

    def __iter__(self):
        """
        迭代器
        :return:
        """
        return iter(self.scan_iter())

    def search(self, pattern):
        """
        按照格式来搜索
        :param pattern:
        :return:
        """
        return self.scan_iter(pattern)

    def List(self, cache_key):
        """
        创建列表对象
        :param cache_key:
        :return:
        """
        return List(self, cache_key)

    def Set(self, cache_key):
        """
        创建集合对象
        :param cache_key:
        :return:
        """
        return Set(self, cache_key)

    def ZSet(self, cache_key):
        """
        创建有序集合对象
        :param cache_key:
        :return:
        """
        return ZSet(self, cache_key)

    def Hash(self, cache_key):
        """
        创建哈希对象
        :param cache_key:
        :return:
        """
        return Hash(self, cache_key)

    def HyperLogLog(self, cache_key):
        """
        创建基数统计对象
        :param cache_key:
        :return:
        """
        return HyperLogLog(self, cache_key)

    def Counter(self, cache_key, expire_time=60 * 60 * 24):
        """
        创建计数器对象
        :param cache_key:
        :param expire_time:
        :return:
        """
        return Counter(self, cache_key, expire_time)

    def RateLimit(self, cache_key, limit=5, per=60, ret=None):
        """
        创建频率限制对象
        :param cache_key:
        :param limit:
        :param per:
        :param ret:
        :return:
        """
        return RateLimit(self, cache_key, limit, per, ret)
