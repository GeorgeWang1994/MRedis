# GRedis

Lightweight Python utilities for working with [Redis](http://redis.io).

GRedis是为了能够在Python中更加方便快捷的使用Redis而开发的，而且尽量让开发人员避免去重新的
学习一个库。GRedis是基于`redis-py`的基础上开发的。

## 数据结构与封装

GRedis的数据结构包括：

* Hash
* Set
* Sorted Set
* List
* HyperLogLog
* Stream
* ConsumerGroup

同时还提供了业务层面的封装，包括：

* Counter
* Lock
* RateLimit
* BadgeManager

## 如何使用

### Hash

```python

from gredis.database import GRedis

gredis = GRedis(host='localhost', port=6379, decode_responses=True) 
hash = gredis.Hash('test_hash')

hash["first"] = 1
hash.update({"second": 2, "third": 3})
assert len(hash) == 3
assert "first" in hash

del hash['first']
assert "first" not in hash

keys = ['first', 'second', 'third']
for key, val in hash:
    assert key in keys
    assert hash[key] == val
    
```


### List

```python

from gredis.database import GRedis

gredis = GRedis(host='localhost', port=6379, decode_responses=True) 
set1 = gredis.Set('test_set1')
set2 = gredis.Set('test_set2')

set1.update({1, 2, 3})
set2.update({2, 3, 4})

result = set1.intersection(set2)
assert set(result) == {'2', '3'}

result = set1.union(set2)
assert set(result) == {'1', '2', '3', '4'}

result = set1.difference(set2)
assert set(result) == {'1'}

```


### SortedSet

```python

from gredis.database import GRedis

gredis = GRedis(host='localhost', port=6379, decode_responses=True) 
sorted_set = gredis.SortedSet('test_sorted_set')

sorted_set.append({'third': 3, 'second': 2, 'first': 1})

result = sorted_set.range(0, 1)
assert result == ['first', 'second']

result = sorted_set.range_by_score(1, 2)
assert result == ['first', 'second']

sorted_set.append({'third': 3, 'second': 2, 'first': 1})
for member, score in sorted_set:
    assert member
    assert score

```


### List

```python

from gredis.database import GRedis

gredis = GRedis(host='localhost', port=6379, decode_responses=True) 
list = gredis.List('test_list')

list.append(10)
list.extend([20, 30, 40])
list.prepend(0)
assert len(list) == 5
result = list[1: 4]
assert result == ['10', '20', '30', '40']

```
