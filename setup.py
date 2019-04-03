# -*- coding: UTF-8 -*-

from setuptools import find_packages
from setuptools import setup

setup(
    name="GRedis",
    description="GRedis",
    auth="george wang",
    author_email="georgewang1994@163.com",
    install_requires=['redis>=3.0.0'],
    packages=find_packages(),
)
