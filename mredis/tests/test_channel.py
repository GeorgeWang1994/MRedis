# -*- coding: utf-8 -*-

# @Time    : 2019/5/1 2:16 PM
# @Author  : George
# @File    : test_channel.py
# @Contact : georgewang1994@163.com


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


if __name__ == '__main__':
    server_thread = TestServerProcess(name="server_thread")
    client_thread = TestClientProcess(name="client_thread")
    server_thread.start()
    client_thread.start()

    server_thread.join()
    client_thread.join()
