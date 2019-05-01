# -*- coding: UTF-8 -*-
import functools
import hashlib
import time

import simplejson


class RateLimit(object):
    """
    频率限制
    """
    def __init__(self, database, cache_key, limit=5, per=60, ret=None):
        """

        :param database:
        :param cache_key:
        :param limit: 访问次数
        :param per: 时间频率
        """
        self.database = database
        self.cache_key = cache_key
        self._limit = limit
        self._per = per
        self._ret = ret

    def limit(self, key):
        """
        是否限制关键字
        :param key:
        :return:
        """
        counter = self.database.List(key)
        ll = len(counter)
        if ll < self._limit:
            counter.prepend(str(time.time()))
        else:
            old_time = float(counter[-1])
            if time.time() - old_time < self._per:
                return True
            del counter[-1:]
            counter.prepend(str(time.time()))
        return False

    def rate_limit(self, function=None):
        """
        函数限制的装饰器
        :param function:
        :return:
        """
        if function is None:
            def function_key(*args, **kwargs):
                data = simplejson.dumps((args, sorted(kwargs.items())))
                return hashlib.md5(data).hexdigest()

        def decorator(fn):
            @functools.wraps(fn)
            def wrapper(*args, **kwargs):
                func_key = function_key(*args, **kwargs)
                if self.limit(func_key):
                    return self._ret
                return fn(*args, **kwargs)
            return wrapper
        return decorator
