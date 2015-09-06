#!/usr/bin/env python

from setuptools import setup, find_packages

version = '0.0.1'

setup(
    name='tornmem',
    version=version,
    description="Asynchronous Memcached client that works within Tornado IO loop",
    packages=['tornmem'],
    author="Eddy Zhou",
    author_email="zhouqian1103@gmail.com",
    license="http://www.apache.org/licenses/LICENSE-2.0",
    url="https://github.com/eddyzhou/tornado-memcached",
)
