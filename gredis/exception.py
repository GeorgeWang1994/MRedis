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
