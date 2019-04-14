# -*- coding: utf-8 -*-

# @Time    : 2019/4/14 4:00 PM
# @Author  : George
# @File    : badge.py
# @Contact : georgewang1994@163.com


class BadgeManager(object):
    """
    badge管理
    """
    def __init__(self, database, basic_cache_key, expire_time=60 * 60):
        self.database = database
        self.basic_cache_key = basic_cache_key
        self.expire_time = expire_time

    def _build_cache_key(self, unique_id):
        return "%s_%s" % (self.basic_cache_key, unique_id)

    def incr_badge(self, unique_id, service_id):
        """
        增加badge
        :param unique_id:
        :param service_id:
        :return:
        """
        cache_key = self._build_cache_key(unique_id)
        self.database.sadd(cache_key, service_id)
        self.database.expire(cache_key, self.expire_time)

    def decr_badge(self, unique_id, service_id):
        """
        清除badge
        :param unique_id:
        :param service_id:
        :return:
        """
        cache_key = self._build_cache_key(unique_id)
        self.database.srem(cache_key, service_id)

    def delete(self, unique_id):
        """
        删除badge
        :param unique_id:
        :return:
        """
        cache_key = self._build_cache_key(unique_id)
        self.database.delete(cache_key)

    def has_badge(self, unique_id):
        """
        判断是否有badge
        :param unique_id:
        :return:
        """
        cache_key = self._build_cache_key(unique_id)
        return self.database.exist(cache_key)

    def has_service_badge(self, unique_id, service_id):
        """
        判断是否有对应的badge
        :param unique_id:
        :param service_id:
        :return:
        """
        cache_key = self._build_cache_key(unique_id)
        return self.database.sismember(cache_key, service_id)

    def badge_count(self, unique_id):
        """
        badge个数
        :return:
        """
        cache_key = self._build_cache_key(unique_id)
        return self.database.scard(cache_key)

    def service_id_list(self, unique_id):
        """
        服务id列表
        :return:
        """
        cache_key = self._build_cache_key(unique_id)
        return self.database.smembers(cache_key)
