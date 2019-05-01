# MRedis

Lightweight Python utilities for working with [Redis](http://redis.io).

MRedis是为了能够在Python中更加方便快捷的使用Redis而开发的，而且尽量让开发人员避免去重新的
学习一个库。MRedis是基于`redis-py`的基础上开发的。

## 数据结构与封装

MRedis的数据结构包括：

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

from mredis.database import MRedis

mredis = MRedis(host='localhost', port=6379, decode_responses=True) 
hash = mredis.Hash('test_hash')

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

from mredis.database import MRedis

mredis = MRedis(host='localhost', port=6379, decode_responses=True) 
set1 = mredis.Set('test_set1')
set2 = mredis.Set('test_set2')

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

from mredis.database import MRedis

mredis = MRedis(host='localhost', port=6379, decode_responses=True) 
sorted_set = mredis.SortedSet('test_sorted_set')

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

from mredis.database import MRedis

mredis = MRedis(host='localhost', port=6379, decode_responses=True) 
list = mredis.List('test_list')

list.append(10)
list.extend([20, 30, 40])
list.prepend(0)
assert len(list) == 5
result = list[1: 4]
assert result == ['10', '20', '30', '40']

```

### MQ

```python

from multiprocessing import Process

import simplejson as json
from redis import Redis

from mredis.channel import Client, Server

redis = Redis(host='localhost', port=6379, decode_responses=True)


class TestClientProcess(Process):
    def __init__(self, *args, **kwargs):
        super(TestClientProcess, self).__init__(*args, **kwargs)
        self.client = Client(redis, "test_channel")

    def run(self):
        print("客户端开始执行")
        self.handel()
        print("客户端执行结束")

    def handel(self):
        for value in range(1, 10):
            req_id = self.client.send_req(value)
            print(u'客户端开始发送消息:%s,%s' % (req_id, value))
            rsp_value = self.client.brecv_rsp(req_id, timeout=10)
            if rsp_value:
                print(u'客户端获取到的结果是:%s' % rsp_value)
                self.client.ack_rsp(req_id, rsp_value)


class TestServerProcess(Process):
    def __init__(self, *args, **kwargs):
        super(TestServerProcess, self).__init__(*args, **kwargs)
        self.server = Server(redis, "test_channel")

    def run(self):
        print("服务器开始执行")
        self.handel()
        print("服务器执行结束")

    def handel(self):
        while True:
            req_id, req_value = self.server.brecv_req(timeout=10)
            print(u'服务器开始接受消息:%s,%s' % (req_id, req_value))
            if req_id:
                print(u'服务器端获取到的结果是:%s,%s' % (req_id, req_value))
                if self.server.send_rsp(req_id, json.dumps({"data": req_value})):
                    self.server.ack_req(req_id, req_value)

```

