# -*- coding: utf-8 -*-

# @Time    : 2019/4/13 5:14 PM
# @Author  : George
# @File    : lock.py
# @Contact : georgewang1994@163.com

import functools
import hashlib
import os

from mako.template import Template

from mredis.exception import LockReleaseException


class Lock(object):
    """
    一个共享的分布式锁，该锁用lua脚本保证原子性操作，可以设置expire_times时间避免死锁的发生

    使用lua脚本的好处：

    1.减少网络开销：本来x5次网络请求的操作，可以用一个请求完成，原先5次请求的逻辑放在redis服务器上完成。使用脚本，减少了网络往返时延。
    2.原子操作：Redis会将整个脚本作为一个整体执行，中间不会被其他命令插入。
    3.复用：客户端发送的脚本会永久存储在Redis中，意味着其他客户端可以复用这一脚本而不需要使用代码完成同样的逻辑。

    """
    lua_release = None
    lua_add = None
    lua_acquire = None

    def __init__(self, database, cache_key):
        self.database = database
        self.cache_key = cache_key
        self.base_dir = os.path.dirname(os.path.abspath(__file__)) + '/scripts/'

        self.register_scripts()

    def get_script(self, lua_name):
        return Template(filename=lua_name).render()

    def register_scripts(self):
        """
        注册lua命令
        :return:
        """
        if self.lua_acquire is None:
            self.lua_acquire = self.database.register_script(self.get_script(self.base_dir + 'lock_acquire.lua'))
        if self.lua_release is None:
            self.lua_release = self.database.register_script(self.get_script(self.base_dir + 'lock_release.lua'))
        if self.lua_add is None:
            self.lua_add = self.database.register_script(self.get_script(self.base_dir + 'lock_add.lua'))

    @property
    def _value(self):
        """
        key值
        :return:
        """
        md5 = hashlib.md5()
        md5.update((self.cache_key.encode('utf-8')))
        return md5.hexdigest()

    @property
    def event_key(self):
        """
        事件的key
        :return:
        """
        return "lock_event_%s" % self.cache_key

    def acquire(self, block_timeout=1, expire_time=1):
        """
        获取锁
        :param block_timeout: 阻塞获取锁的时间
        :param expire_time: 设置锁的时间
        :return:
        """
        while True:
            result = self.lua_acquire(keys=[self.cache_key], args=[self._value, expire_time])
            if result:
                return True

            result = self.database.blpop(self.event_key, block_timeout)
            return result is not None

    def release(self):
        """
        释放锁
        :return:
        """
        return bool(self.lua_release(keys=[self.cache_key, self.event_key]))

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
