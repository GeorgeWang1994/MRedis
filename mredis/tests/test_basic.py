# -*- coding: utf-8 -*-

# @Time    : 2019/4/27 1:05 PM
# @Author  : George
# @File    : test_basic.py
# @Contact : georgewang1994@163.com


import unittest

from mredis.database import MRedis


class TestBasic(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        self.mredis = MRedis(host='localhost', port=6379, decode_responses=True)
        super(TestBasic, self).__init__(*args, **kwargs)

    def setUp(self):
        self.mredis.flushdb()

    def tearDown(self):
        self.mredis.flushdb()
