# -*- coding: utf-8 -*-

# @Time    : 2019/4/30 6:31 PM
# @Author  : George
# @File    : channel.py
# @Contact : georgewang1994@163.com

import hashlib


class Queue(object):
    """
    消息队列
    """
    def __init__(self, database, cache_key):
        self.database = database
        self.cache_key = cache_key

    def _build_queue_cache_key(self, is_assess=True):
        """
        构建队列cache的key
        :param is_assess:
        :return:
        """
        return ("%s_assess" if is_assess else "%s_process") % self.cache_key

    def __len__(self):
        return self.database.llen(self._build_queue_cache_key()) + \
               self.database.llen(self._build_queue_cache_key(False))

    def push(self, _value):
        """
        添加消息
        :param _value:
        :return:
        """
        return self.database.lpush(self._build_queue_cache_key(), _value)

    def pop(self):
        """
        弹出消息，为了队列的安全，避免pop出来遇见异常导致数据丢失，加入到另外的一个队列中
        :return:
        """
        return self.database.rpoplpush(self._build_queue_cache_key(), self._build_queue_cache_key(False))

    def bpop(self, timeout=0):
        """
        阻塞时间直到消息弹出，默认0表示一直阻塞
        :param timeout:
        :return:
        """
        return self.database.brpoplpush(self._build_queue_cache_key(), self._build_queue_cache_key(False), timeout)

    def ack(self, _value):
        """
        确认消息
        :return:
        """
        return self.database.lrem(self._build_queue_cache_key(False), 1, _value)


class Channel(object):
    """
    消息频道
    """
    def __init__(self, database, cache_key):
        self.database = database
        self.cache_key = cache_key
        self.req_queue = Queue(self.database, "channel_request_queue_%s" % self.cache_key)
        self.pubsub = self.database.pubsub(ignore_subscribe_messages=True)  # 不需要关注订阅和取消订阅消息
        self.subscriber = None

    def req_id(self, value):
        return hashlib.md5(("request_" + self.cache_key + str(value)).encode("utf-8")).hexdigest()

    def rsp_queue_id(self, req_id):
        return "channel_response_queue_%s:%s" % (self.cache_key, req_id)

    def close(self):
        """
        关闭频道
        :return:
        """
        if self.subscriber:
            self.subscriber.stop()
        self.pubsub.unsubscribe()
        self.pubsub.close()


class Client(Channel):
    """
    客户端
    """
    def __init__(self, database, cache_key):
        super(Client, self).__init__(database, cache_key)

    def send_req(self, value):
        """
        发送请求
        :param value:
        :return:
        """
        _id = self.req_id(value)
        if self.database.set(_id, value):
            if self.req_queue.push(_id):
                return _id
        return 0

    def brecv_rsp(self, req_id, timeout=0):
        """
        阻塞获取结果
        :param req_id:
        :param timeout:
        :return:
        """
        rsp_queue = Queue(self.database, self.rsp_queue_id(req_id))
        return rsp_queue.bpop(timeout)

    def recv_rsp(self, req_id):
        """
        获取结果
        :param req_id
        :return:
        """
        rsp_queue = Queue(self.database, self.rsp_queue_id(req_id))
        return rsp_queue.pop()

    def ack_rsp(self, req_id, value):
        """
        确认结果
        :param req_id:
        :param value:
        :return:
        """
        rsp_queue = Queue(self.database, self.rsp_queue_id(req_id))
        return rsp_queue.ack(value)

    def set_rsp_handler(self, handler):
        """
        在单独的线程里运行一个事件循环，但是没有办法控制不是由注册的消息控制器自动控制的消息
        :param handler:
        :return:
        """
        self.pubsub.subscribe(**{"response_handler_%s" % self.cache_key: handler})
        if not self.subscriber:
            self.subscriber = self.pubsub.run_in_thread(sleep_time=0.001)

    def reset_rsp_handler(self,):
        """
        重置消息控制器
        :return:
        """
        self.pubsub.unsubscribe("response_handler_%s" % self.cache_key)
        self.subscriber = None


class Server(Channel):
    """
    服务器
    """
    def __init__(self, database, cache_key):
        super(Server, self).__init__(database, cache_key)

    def brecv_req(self, timeout=0):
        """
        接受请求
        :param timeout:
        :return:
        """
        req_id = self.req_queue.bpop(timeout)
        if req_id:
            return req_id, self.database.get(req_id)

        return 0, None

    def ack_req(self, req_id, value):
        """
        确认接受到请求
        :param req_id:
        :param value:
        :return:
        """
        if self.req_queue.ack(req_id):
            self.database.delete(self.req_id(value))

    def send_rsp(self, req_id, value):
        """
        发送回应
        :param req_id:
        :param value:
        :return:
        """
        rsp_queue = Queue(self.database, self.rsp_queue_id(req_id))
        if rsp_queue.push(value):
            self.database.publish("response_%s" % self.cache_key, req_id)
            return True
        return False
