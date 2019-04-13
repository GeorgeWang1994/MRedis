# -*- coding: utf-8 -*-

# @Time    : 2019/4/13 5:14 PM
# @Author  : George
# @File    : lock.py
# @Contact : georgewang1994@163.com

import functools
import hashlib

from exception import LockReleaseException


class Lock(object):
    """
    一个共享的分布式锁，该锁用lua脚本保证原子性操作，可以设置ttl时间避免死锁的发生
    """
    lua_release = None
    lua_add = None
    lua_acquire = None

    def __init__(self, database, cache_key, ttl=None):
        self.database = database
        self.cache_key = cache_key
        self.ttl = ttl or 0
        self.register_scripts()

    def register_scripts(self):
        """
        注册lua命令
        :return:
        """
        if self.lua_acquire is None:
            self.lua_acquire = self.database.register_script('lock_acquire')
        if self.lua_release is None:
            self.lua_release = self.database.register_script('lock_release')
        if self.lua_add is None:
            self.lua_add = self.database.register_script('lock_add')

    @property
    def _value(self):
        """
        key值
        :return:
        """
        return hashlib.md5(self.cache_key)

    @property
    def event_key(self):
        """
        事件的key
        :return:
        """
        return "lock_event_%s" % self.cache_key

    def acquire(self, block=True):
        """
        获取锁
        :param block:
        :return:
        """
        while True:
            result = self.lua_acquire(keys=[self.cache_key, self._value], args=[self.ttl])
            if result and not block:
                return True

            result = self.database.blpop(self.event_key, self.ttl)
            if result is not None:
                return result is not None

    def release(self):
        """
        释放锁
        :return:
        """
        return bool(self.lua_release(keys=[self.cache_key, self.event_key]))

    def add(self, ttl):
        """
        在原来的剩余生存时间的基础上增加生存时间
        :param ttl:
        :return:
        """
        result = self.lua_add(keys=[self.cache_key, self._value])
        if result:
            self.ttl += ttl
        return result

    def cur_ttl(self):
        """
        获取ttl
        :return:
        """
        return self.database.ttl(self.cache_key)

    def clear(self):
        self.database.delete(self.cache_key)
        self.database.delete(self.event_key)

    def __enter__(self):
        self.acquire()

    def __exit__(self, exc_type, exc_val, exc_tb):
        if not self.release():
            raise LockReleaseException(u'锁释放失败')

    def __call__(self, func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            with self:
                return func(*args, **kwargs)
        return wrapper
