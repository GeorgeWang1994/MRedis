# -*- coding: UTF-8 -*-

import functools
import hashlib
from collections import Iterable

from redis import Redis

from mredis.badge import BadgeManager
from mredis.containers import List, Set, SortedSet, Hash, HyperLogLog, Container
from mredis.counter import Counter
from mredis.lock import Lock
from mredis.rate_limit import RateLimit


class MRedis(Redis):
    """
    redis客户端
    """
    def __init__(self, *args, **kwargs):
        super(MRedis, self).__init__(*args, **kwargs)

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

    def SortedSet(self, cache_key):
        """
        创建有序集合对象
        :param cache_key:
        :return:
        """
        return SortedSet(self, cache_key)

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

    def Lock(self, cache_key, expire_times=60 * 60 * 24):
        """
        创建锁
        :param cache_key:
        :param expire_times:
        :return:
        """
        return Lock(self, cache_key, expire_times)

    def BadgeManager(self, basic_cache_key, expire_time=60 * 60 * 24):
        """
        创建badge管理
        :param basic_cache_key:
        :param expire_time:
        :return:
        """
        return BadgeManager(self, basic_cache_key, expire_time)

    def _get_func_cache_key_id(self, func, args, kwargs):
        """
        函数缓存的key
        :return:
        """
        str_value = (func.__name__ + str(args) + str(kwargs)).encode('utf-8')
        md5 = hashlib.md5()
        md5.update(str_value)
        return md5.hexdigest()

    def func_cache(self, expire_times=60 * 60):
        """
        函数缓存实现
        :param expire_times:
        :return:
        """
        def wrapper(func):
            @functools.wraps(func)
            def _wrapper(*args, **kwargs):
                cache_key = self._get_func_cache_key_id(func, args, kwargs)
                container = Container(database=self, cache_key=cache_key)
                res = container.get_pickle()
                if not res:
                    res = func(*args, **kwargs)
                    container.set_pickle(res, expire_times)
                return res

            return _wrapper

        return wrapper

    def _get_func_mutex_key_id(self, func, lock_params):
        """
        函数锁的key
        :return:
        """
        str_value = (func.__name__+ str(lock_params)).encode('utf-8')
        md5 = hashlib.md5()
        md5.update(str_value)
        return md5.hexdigest()

    def func_mutex(self, lock_params=(), default=None, lock_times=1):
        """
        函数锁实现
        :param lock_params: 根据指定的参数加锁
        :param default: 默认返回值
        :param lock_times: 默认锁的时间
        :return:
        """
        if not isinstance(lock_params, Iterable):
            raise Exception("参数类型错误")

        def wrapper(func):
            @functools.wraps(func)
            def _wrapper(*args, **kwargs):
                cache_key = self._get_func_mutex_key_id(func, lock_params)
                lock = Lock(database=self, cache_key=cache_key)
                if lock.acquire(expire_time=lock_times):
                    res = func(*args, **kwargs)
                    lock.release()
                    return res
                return default
            return _wrapper
        return wrapper
