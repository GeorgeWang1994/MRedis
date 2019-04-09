# -*- coding: UTF-8 -*-
from collections import Iterable


class Sortable(object):
    """
    sortable for redis
    """
    def __init__(self, database=None, cache_key=None):
        self.database = database
        self.cache_key = cache_key

    def sort(self, by_pattern=None, start=None, num=None, get_pattern=None, is_desc=False,
             is_alpha=False, store=None):
        return self.database.sort(
            self.cache_key, by=by_pattern, start=start, num=num, get=get_pattern,
            desc=is_desc, alpha=is_alpha, store=store,
        )


class Container(object):
    """
    基础空间
    """
    def __init__(self, database=None, cache_key=None):
        self.database = database
        self.cache_key = cache_key

    def delete(self):
        """
        删除key
        :return:
        """
        self.database.delete(self.cache_key)

    def expire(self, seconds=None, timestamp=None):
        """
        设置过期时间，如果什么都没有设置的话，则默认为永久保存
        :param seconds:
        :param timestamp:
        :return:
        """
        if seconds:
            self.database.expire(self.cache_key, seconds)
        elif timestamp:
            self.database.expireat(self.cache_key, timestamp)
        else:
            self.database.persist(self.cache_key)

    def rename(self, new_key, check=True):
        """
        对key进行重命名
        :param new_key:
        :param check:
        :return:
        """
        if check:
            self.database.renamenx(self.cache_key, new_key)
        else:
            self.database.rename(self.cache_key, new_key)

    def left_seconds(self):
        """
        剩余的时间
        :return:
        """
        return self.database.ttl(self.cache_key)


class Hash(Container):
    """
    哈希
    """
    def __repr__(self):
        return self.get_self()

    def clear(self):
        """
        清除所有的键值对，并且把自己给删除
        """
        self.delete()

    def _del_key(self, *args):
        self.database.hdel(self.cache_key, args)

    def get(self, key, default=None):
        """
        获取值
        :param key:
        :param default:
        :return:
        """
        if key not in self:
            return default
        return self.database.hget(self.cache_key, key)

    def __getitem__(self, key):
        """
        获取值
        :param key:
        :return:
        """
        return self.get(key)

    def has_key(self, key):
        """
        键是否存在
        :param key:
        :return:
        """
        return self.database.hexist(self.cache_key, key)

    def __contains__(self, key):
        """
        键是否存在
        :param key:
        :return:
        """
        return self.database.hexist(self.cache_key, key)

    def get_self(self):
        """
        获取字典内容
        :return:
        """
        return self.database.hgetall(self.cache_key)

    def items(self):
        """
        获取所有键值对
        :return:
        """
        return self.database.hgetall(self.cache_key)

    def _scan(self, cursor=0, pattern=None, count=None):
        """
        分片读取，避免一次获取大量的数据导致内存被挤爆
        :param cursor:
        :param pattern:
        :param count:
        :return:
        """
        return self.database.hscan_iter(self.cache_key, cursor, pattern, count)

    def __iter__(self):
        """
        迭代器
        :return:
        """
        return iter(self._scan())

    def iteritems(self):
        """
        获取所有的键值对
        """
        return self._scan()

    def search(self, pattern, count=None):
        """
        搜索
        :param pattern:
        :param count:
        :return:
        """
        return self._scan(pattern=pattern, count=count)

    def keys(self):
        """
        获取所有的key
        :return:
        """
        return self.database.hkeys(self.cache_key)

    def values(self):
        """
        获取所有的值
        """
        return self.database.hvals(self.cache_key)

    def pop(self, key, default=None):
        """
        弹出指定的key
        :param key:
        :param default:
        :return:
        """
        if key not in self:
            if default:
                return default
            else:
                raise KeyError(u'not found the key')

        value = self.get(key)
        self._del_key([key])
        return value

    def popitem(self):
        """
        弹出任意的键值对
        :return:
        """
        if not self:
            raise KeyError(u'empty hash')

        rand_key, rand_val = next(self.iteritems())
        return rand_key, self.pop(rand_key)

    def __delitem__(self, key):
        self._del_key(key)

    def __setitem__(self, key, value):
        """
        设置键值对
        """
        self.database.hset(self.cache_key, key, value)

    def setdefault(self, key, default=None):
        """
        设置默认值
        :param key:
        :param default:
        :return:
        """
        value = self.get(key)
        if not value:
            self[key] = default
        return value

    def update(self, other):
        """
        更新键值对
        :param other:
        :return:
        """
        if not other:
            return
        self.database.hmset(self.cache_key, *(other.items()))

    def __eq__(self, other):
        """
        判断是否相等
        :param other:
        :return:
        """
        cur = self.get_self()
        return cur == other

    def __cmp__(self, other):
        """
        进行比较
        :param other:
        :return:
        """
        cur = self.get_self()
        return cmp(cur, other)

    def __len__(self):
        """
        获取长度
        :return:
        """
        return self.database.hlen(self.cache_key)

    def __le__(self, other):
        """
        判断小于等于
        :param other:
        :return:
        """
        cur = self.get_self()
        return cur <= other

    def __ge__(self, other):
        """
        判断大于等于
        :param other:
        :return:
        """
        cur = self.get_self()
        return cur >= other

    def __ne__(self, other):
        """
        判断不等于
        :param other:
        :return:
        """
        cur = self.get_self()
        return cur != other


class Set(Sortable, Container):
    """
    集合
    """
    def add(self, val):
        """
        添加值
        :param val:
        :return:
        """
        self.database.sadd(self.cache_key, val)

    def clear(self):
        """
        清除所有的键值对，并且把自己给删除
        """
        self.delete()

    def get_self(self):
        """
        获取集合内容
        """
        return self.database.smembers(self.cache_key)

    def difference(self, other, with_score=False):
        """
        取差集
        :param other:
        :param with_score:
        :return:
        """
        if isinstance(other, Set):
            values = other.get_self()
        elif isinstance(other, set):
            values = other
        else:
            raise TypeError('parameter format error')

        if with_score:
            return self.database.sdiffscore(self.cache_key, values)
        return self.database.sdiff(self.cache_key, values)

    def difference_store(self, dest_key, *args):
        """
        将差集的结果存储到新的key中
        :param dest_key:
        :param args:
        :return:
        """
        return self.database.sdiffstore(self.cache_key, dest_key, args)

    def discard(self, val):
        """
        删除值
        :param val:
        :return:
        """
        self.database.srem(self.cache_key, val)

    def intersection(self, other, with_score=False):
        """
        取交集
        :param other:
        :param with_score:
        :return:
        """
        if isinstance(other, Set):
            values = other.get_self()
        elif isinstance(other, set):
            values = other
        else:
            raise TypeError('parameter format error')

        if with_score:
            return self.database.sinterscore(self.cache_key, values)
        return self.database.sinter(self.cache_key, values)

    def intersection_store(self, dest_key, *args):
        """
        将交集的结果存储到新的key中
        :param dest_key:
        :param args:
        :return:
        """
        return self.database.sinterstore(self.cache_key, dest_key, args)

    def _scan(self, pattern=None, count=None):
        """
        分片读取，避免一次获取大量的数据导致内存被挤爆
        :param pattern:
        :param count:
        :return:
        """
        return self.database.zscan_iter(self.cache_key, pattern, count)

    def issubset(self, other):
        """
        判断是否是子集
        :param other:
        :return:
        """
        cur = self.get_self()
        return cur.issubset(other)

    def issuperset(self, other):
        """
        判断是否是父集
        :param other:
        :return:
        """
        cur = self.get_self()
        return cur.issuperset(other)

    def pop(self, count=1):
        """
        随机弹出值
        :param count:
        :return:
        """
        return self.database.spop(self.cache_key, count)

    def remove(self, *args):
        """
        删除元素
        :param args:
        :return:
        """
        self.database.srem(self.cache_key, args)

    def union(self, other, with_score=False):
        """
        取并集
        :param other:
        :param with_score:
        :return:
        """
        if isinstance(other, Set):
            values = other.get_self()
        elif isinstance(other, set):
            values = other
        else:
            raise TypeError('parameter format error')

        if with_score:
            return self.database.sunionstore(self.cache_key, values)
        return self.database.sunion(self.cache_key, values)

    def union_store(self, dest_key, *args):
        """
        将并集的结果存储到新的key中
        :param dest_key:
        :param args:
        :return:
        """
        return self.database.sunionstore(self.cache_key, dest_key, args)

    def update(self, other):
        """
        并集
        :param other:
        :return:
        """
        return self.union(other)

    def rand(self, count=None):
        """
        返回指定个数的值
        :param count:
        :return:
        """
        return self.database.srandmember(self.cache_key, count)

    def __contains__(self, item):
        """
        判断元素是否存在
        :param item:
        :return:
        """
        return self.database.sismember(self.cache_key, item)

    def __iand__(self, other):
        """
        cur &= other
        """
        return self.intersection(other)

    def __ior__(self, other):
        """
        cur |= other
        """
        return self.union(other)

    def __isub__(self, other):
        """
        cur -= other
        """
        return self.difference(other)

    def __iter__(self):
        """
        迭代器
        :return:
        """
        return iter(self._scan())

    def __ixor__(self, other):
        """
        cur ^= other
        """
        pass

    def __len__(self):
        """
        获取长度
        """
        return self.database.scard(self.cache_key)


class ZSet(Sortable, Container):
    def get_self(self, is_with_score=False):
        """
        返回有序集合的结果
        :param is_with_score:
        :return:
        """
        return self.range(0, -1, False, is_with_score)

    def append(self, mapping=None, **kwargs):
        """
        添加成员分值，成员到有序集合中
        :param mapping:
        :param kwargs:
        :return:
        """
        if not mapping:
            _mapping = mapping.copy()
            _mapping.update(kwargs)
        else:
            _mapping = mapping
        self.database.zadd(self.cache_key, _mapping)

    def incr(self, member, amount=1.):
        """
        增加指定值
        :param member:
        :param amount:
        :return:
        """
        self.database.zincrby(self.cache_key, amount, member)

    def intersection_store(self, dest_key, *args):
        """
        将交集的结果存储到新的key中
        :param dest_key:
        :param args:
        :return:
        """
        return self.database.zinterstore(self.cache_key, dest_key, args)

    def union_store(self, dest_key, *args):
        """
        将并集的结果存储到新的key中
        :param dest_key:
        :param args:
        :return:
        """
        return self.database.zunionstore(self.cache_key, dest_key, args)

    def pop_max(self, count=1):
        """
        弹出指定个数的最大值（根据分值排序）
        :param count:
        :return:
        """
        return self.database.zpopmax(self.cache_key, count)

    def pop_min(self, count=1):
        """
        弹出指定个数的最小值（根据分值排序）
        :param count:
        :return:
        """
        return self.database.zpopmin(self.cache_key, count)

    def __getitem__(self, item):
        """
        如果是切分则切分，否则返回具体排名的分值
        :param item:
        :return:
        """
        if isinstance(item, slice):
            start = item.start or 0
            stop = item.stop or len(self)
            step = item.step or 1
            if step > 0:
                return self.range(start, stop)
            else:
                return self.range(start, stop, True)
        elif isinstance(item, int):
            return self.database.zscore(self.cache_key, item)
        else:
            raise TypeError(u'parameter format error')

    def __setitem__(self, item, value):
        """
        设置值
        :param key:
        :param value:
        :return:
        """
        self.append({item: value})

    def __delitem__(self, item):
        """
        删除成员
        :param item:
        :return:
        """
        self.remove([item])

    def __delslice__(self, min, max):
        """
        删除成员
        :param min:
        :param max:
        :return:
        """
        self.remove_by_rank(min, max)

    def range(self, start, stop, is_reverse=False, is_desc=False, is_with_scores=False):
        """
        获取指定的范围
        :param start:
        :param stop:
        :param is_reverse:
        :param is_desc:
        :param is_with_scores:
        :return:
        """
        if not is_reverse:
            return self.database.zrange(self.cache_key, start, stop, is_desc, is_with_scores)
        return self.database.zrevrange(self.cache_key, start, stop, is_with_scores)

    def range_by_score(self, min, max, start=None, stop=None, is_reverse=False, is_with_scores=False):
        """
        根据分值来取
        :param min:
        :param max:
        :param start:
        :param stop:
        :param is_reverse:
        :param is_with_scores:
        :return:
        """
        num = None
        if stop and start:
            num = stop - start + 1
        if not is_reverse:
            return self.database.zrangebyscore(min, max, start, num, is_with_scores)
        return self.database.zrevrangebyscore(min, max, start, num, is_with_scores)

    def rank(self, value, is_reverse=False):
        """
        获取排名
        :param value:
        :param is_reverse:
        :return:
        """
        if not is_reverse:
            return self.database.zrank(self.cache_key, value)
        return self.database.zrevrank(self.cache_key, value)

    def score(self, value):
        """
        获取分值
        :param value:
        :return:
        """
        return self.database.zscore(self.cache_key, value)

    def remove(self, *members):
        """
        删除成员
        :param members:
        :return:
        """
        self.database.zrem(self.cache_key, members)

    def remove_by_rank(self, min, max):
        """
        通过排名来删除
        :param min:
        :param max:
        :return:
        """
        self.database.zremrangebyrank(self.cache_key, min, max)

    def remove_by_score(self, min, max):
        """
        通过分值来删除
        :param min:
        :param max:
        :return:
        """
        self.database.zremrangebyscore(self.cache_key, min, max)

    def __repr__(self):
        return self.get_self()

    def __reversed__(self):
        """
        反转
        :return:
        """
        return self.range(0, -1, True)

    def __iadd__(self, other):
        """
        累加
        :param other:
        :return:
        """
        if isinstance(other, ZSet):
            self.append(dict(other.get_self(True)))
        elif isinstance(other, (list, tuple)):
            self.append(dict(other))
        elif isinstance(other, dict):
            self.append(other)
        else:
            raise TypeError(u'parameter format error')

    def __contains__(self, item):
        """
        判断元素是否在有序集合中
        """
        return bool(self.score(item))

    def __iter__(self):
        """
        iter for set
        """
        return iter(self.get_self())

    def __len__(self):
        """
        获取长度
        """
        return self.database.zcard(self.cache_key)


class List(Sortable, Container):
    def get_self(self):
        """
        获取列表内容
        :return:
        """
        return self.database.lrange(self.cache_key, 0, -1)

    def append(self, val):
        """
        往数组末尾添加值
        :param val:
        :return:
        """
        self.database.rpush(self.cache_key, val)

    def prepend(self, val):
        """
        往数组头添加值
        :param val:
        :return:
        """
        self.database.lpush(self.cache_key, val)

    def count(self, val):
        """
        获取值在列表中出现的次数
        :param val:
        :return:
        """
        cur = self.get_self()
        return cur.count(val)

    def extend(self, values):
        """
        添加数组
        :param values:
        :return:
        """
        self.database.rpush(self.cache_key, values)

    def insert(self, index, value):
        """
        往指定位置插入值
        :param index:
        :param value:
        :return:
        """
        self.database.linsert(self.cache_key, index, 'before', value)

    def pop(self, index=None):
        """
        弹出指定位置的值，默认弹出末尾的值
        :param index:
        :return:
        """
        if not self:
            raise TypeError("empty list")

        ll = len(self)
        if index is None or index == ll - 1:
            return self.database.rpop(self.cache_key)
        if index == 0:
            return self.database.lpop(self.cache_key)
        else:
            l = len(self)
            if not (0 <= index < l):
                raise TypeError('index is out of range')

            val = self[index]
            self.database.lset(self.cache_key, index, "__del")
            self.database.lrem(self.cache_key, 1, "__del")
            return val

    def block_pop_left(self, timeout=0):
        """
        阻塞弹出列表头部的值
        :param timeout:
        :return:
        """
        return self.database.blpop(self.cache_key, timeout)

    def block_pop_right(self, timeout=0):
        """
        阻塞弹出列表头部的值
        :param timeout:
        :return:
        """
        return self.database.brpop(self.cache_key, timeout)

    def remove(self, value, count=1):
        """
        删除指定个数的出现的值
        :param value:
        :param count:
        :return:
        """
        if value not in self:
            raise TypeError("not found in the list")
        self.database.lrem(self.cache_key, count, value)

    def __delitem__(self, index):
        """
        删除指定下标的值
        :param index:
        :return:
        """
        self.pop(index)

    def __delslice__(self, start, end):
        """
        删除区间
        :param i:
        :param j:
        :return:
        """
        self.database.ltrim(self.cache_key, start, end)

    def __getitem__(self, item):
        """
        cur[index]
        """
        if isinstance(item, slice):
            start = item.start or 0
            stop = item.stop or len(self)
            return self[start, stop]

        return self[item]

    def __getslice__(self, start, end):
        """
        获取区间的值
        :param start:
        :param end:
        :return:
        """
        return self.database.lrange(self.cache_key, start, end)

    def __iadd__(self, other):
        """
        累加
        :param other:
        :return:
        """
        if isinstance(other, List):
            values = other.get_self()
        elif isinstance(other, Iterable):
            values = other
        else:
            raise TypeError('parameter format error')

        self.extend(values)

    def __iter__(self):
        """
        iter for list
        """
        return iter(self.get_self())

    def __len__(self):
        """
        获取列表长度
        :return:
        """
        return self.database.llen(self.cache_key)

    def __repr__(self):
        return self.get_self()

    def __setitem__(self, index, val):
        """
        设置值
        :param index:
        :param val:
        :return:
        """
        l = len(self)
        if not (0 <= index < l):
            raise TypeError('index is out of range')
        return self.database.lset(self.cache_key, index, val)


class HyperLogLog(Container):
    """
    redis命令hyperloglog
    hyperloglog是用来做基数统计的
    """
    def add(self, *args):
        """
        添加值
        :param args:
        :return:
        """
        self.database.pfadd(self.cache_key, args)

    def count(self):
        """
        获取个数
        :return:
        """
        return self.database.pfcount()

    def __len__(self):
        return self.count()

    def __ior__(self, other):
        if not isinstance(other, Iterable):
            raise TypeError('parameter format error')

        return self.merge(self.cache_key, *other)

    def merge(self, dest, *others):
        """
        合并
        :param dest:
        :param others:
        :return:
        """
        items = [self.cache_key]
        items.extend([other.cache_key for other in others])
        self.database.pfmerge(dest, *items)
        return HyperLogLog(self.database, dest)


class Stream(Container):
    """
    流
    """
    def add(self, mapping, id="*", maxlen=None):
        """
        加入到流中
        :param mapping:
        :param id:
        :param maxlen:
        :return: message's id
        """
        return self.database.xadd(self.cache_key, mapping, id, maxlen)

    def range(self, start='-', end='+', count=None):
        """
        获取指定范围的信息流
        :param start: 最旧的消息
        :param end: 最新的信息
        :param count: 返回的信息个数
        :return:
        """
        return self.database.xrange(self.cache_key, start, end, count)

    def revrange(self, start='+', end='-', count=None):
        """
        获取反向的指定范围的信息流
        :param start: 最新的消息
        :param end: 最旧的消息
        :param count: 返回的信息个数
        :return:
        """
        return self.database.xrevrange(self.cache_key, start, end, count)

    def __getitem__(self, item):
        """
        获取信息
        :param item:
        :return:
        """
        if isinstance(item, slice):
            return self.database.range(item.start, item.stop, item.stop - item.start + 1)

        return self.database.get(item)

    def get(self, msg_id):
        """
        根据id获取信息流
        :param msg_id:
        :return:
        """
        res = self[msg_id:msg_id:1]
        if res:
            return res[0]

    def __len__(self):
        """
        获取长度
        :return:
        """
        return self.database.xlen(self.cache_key)

    def remove(self, msg_ids):
        """
        删除消息
        :param msg_ids:
        :return:
        """
        self.database.xdel(self.cache_key, msg_ids)

    def remove_old(self, count):
        """
        删除旧的消息
        :param count:
        :return:
        """
        self.database.xtrim(self.cache_key, count)

    def remove_group(self, group_key):
        """
        根据消费组名字删除指定消费组
        :param group_key:
        :return:
        """
        self.database.xgroup_destory(self.cache_key, group_key)

    def __delitem__(self, item):
        """
        删除消息id列表
        :param item:
        :return:
        """
        if not isinstance(item, slice):
            item = (item, )
        self.remove(item)

    def read(self, last_id=None, count=None, block=None, is_reverse=False):
        """
        获取信息
        xread命令中的streams指的是 字典表形式{流的名称：消息id}
        :param last_id: 已被读取的最后一条消息的id
        :param count:
        :param block: 阻塞时间（毫秒）
        :param is_reverse: 是否从头读取，默认从头读取
        :return:
        """
        if not last_id:
            if not is_reverse:
                last_id = "0-0"
            else:
                last_id = "$"
        return self.database.xread({self.cache_key: last_id}, count, block)

    def info(self):
        """
        获取流的信息
        :return:
        """
        return self.database.xinfo_stream(self.cache_key)

    def consumers_info(self, group_key):
        """
        返回消费者信息
        :param group_key:
        :return:
        """
        return self.database.xinfo_consumers(self.cache_key, group_key)

    def groups_info(self):
        """
        获取消费组信息
        :return:
        """
        return self.database.xinfo_groups(self.cache_key)


class ConsumerGroup(object):
    def __init__(self, database, cache_key, stream_keys):
        self.database = database
        self.cache_key = cache_key
        self.stream_keys = stream_keys
        for stream_key in stream_keys:
            self._create(stream_key)

    def _create(self, stream_key, start_id='$'):
        """
        在steam上创建新的消费组
        :param stream_key: 消息流的key
        :param start_id: 起始消息id，默认从尾部开始消费，只接受新消息，当前Stream消息会全部忽略
        :return: 最近被消费的id
        """
        return self.database.xgroup_create(stream_key, self.cache_key, start_id)

    def remove_consumer(self, stream_key, consumer_key):
        """
        从消费组移除消费者
        :param stream_key:
        :param consumer_key:
        :return:
        """
        self.database.xgroup_delconsumer(stream_key, self.cache_key, consumer_key)

    def delete(self):
        """
        销毁消费组
        :return:
        """
        for stream_key in self.stream_keys:
            self.database.xgroup_destroy(stream_key, self.cache_key)

    def set_id(self, last_id='$'):
        """
        设置消费者最近一次的消费id
        :param last_id:最近一次消费的id
        :return:
        """
        for stream_key in self.stream_keys:
            self.database.xgroup_setid(stream_key, self.cache_key, last_id)

    def streams_info(self):
        """
        获取流的信息
        :return:
        """
        res = {}
        for stream_key in self.stream_keys:
            res[stream_key] = self.database.xinfo_stream(stream_key)
        return res

    def read(self, consumer_key, count=None, block=None, noack=False):
        """
        从消费者中读取信息
        :param consumer_key:
        :param count:
        :param block:
        :param noack:
        :return:
        """
        return self.database.xreadgroup(self.cache_key, consumer_key, self.stream_keys, count, block)

    def pending(self):
        """
        消费组中待处理的消息
        :return:
        """
        res = {}
        for stream_key in self.stream_keys:
            res[stream_key] = self.database.xpending(stream_key, self.cache_key)
        return res

    def pending_range(self, min_stream_id, max_stream_id, count, consumer_key=None):
        """
        获取消费组中指定范围的待处理消息
        :param min_stream_id:
        :param max_stream_id:
        :param count:
        :param consumer_key:
        :return:
        """
        res = {}
        for stream_key in self.stream_keys:
            res[stream_key] = self.database.xpending_range(stream_key, self.cache_key, min_stream_id,
                                                           max_stream_id, count, consumer_key)
        return res
