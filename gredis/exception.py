# -*- coding: UTF-8 -*-


class RateLimitException(Exception):
    """
    频率限制
    """
    pass


class LockReleaseException(Exception):
    """
    锁释放
    """
    pass


class HashTypeException(Exception):
    """
    哈希类型错误
    """
    pass


class HashKeyException(Exception):
    """
    哈希key错误
    """
    pass


class SetTypeException(Exception):
    """
    集合类型错误
    """
    pass


class SortedSetTypeException(Exception):
    """
    有序集合类型错误
    """
    pass


class ListEmptyException(Exception):
    """
    列表空错误
    """
    pass


class ListNotExistException(Exception):
    """
    列表不存在元素错误
    """
    pass


class ListTypeException(Exception):
    """
    列表类型错误
    """
    pass


class ListIndexErrorException(Exception):
    """
    列表越界错误
    """
    pass
