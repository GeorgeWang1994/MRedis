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
