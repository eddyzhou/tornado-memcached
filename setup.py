from setuptools import setup

version = '0.0.1'

setup(
    name='tornmc',
    version=version,
    description='Asynchronous Memcached client '
                'that works within Tornado IO loop',
    packages=['tornmc'],
    author='Eddy Zhou',
    author_email='zhouqian1103@gmail.com',
    url='https://github.com/eddyzhou/tornado-memcached',
)
