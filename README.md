## GRedis

Lightweight Python utilities for working with [Redis](http://redis.io).

The purpose of it is to make working with Redis in Python a little easier.
Rather than ask you to learn a new library, GRedis subclasses and extends
the popular `redis-py` client, allowing it to be used as a drop-in replacement.

GRedis consists of:

* Pythonic container classes for the Redis data-types:
    * Hash
    * List
    * Set
    * Sorted Set
