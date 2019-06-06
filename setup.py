# -*- coding: UTF-8 -*-

from setuptools import find_packages
from setuptools import setup

setup(
    name="MRedis",
    version="0.2",
    description="redis wrapper",
    author="georgewang",
    author_email="georgewang1994@163.com",
    install_requires=['redis>=3.0.0', 'simplejson'],
    url='https://github.com/GeorgeWang1994/MRedis',
    packages=find_packages(),
    license='MIT',
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
    ],
    keywords='redis client python',
    test_suite='mredis.tests',
)
