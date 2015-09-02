from setuptools import setup, find_packages

version = '0.0.1'

setup(
    name='tornmem',
    version=version,
    description="async memcached client base on tornado io-loop",
    packages=['tornmem'],
    author="Eddy Zhou",
    author_email="zhouqian1103@gmail.com",
    url="https://github.com/eddyzhou/tornado-memcached",
)
