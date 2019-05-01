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


class TypeException(Exception):
    """
    类型错误
    """
    pass


class KeyErrorException(Exception):
    """
    找不到key错误
    """
    pass


class EmptyException(Exception):
    """
    空错误
    """
    pass


class NotExistException(Exception):
    """
    不存在元素错误
    """
    pass


class IndexErrorException(Exception):
    """
    越界错误
    """
    pass
