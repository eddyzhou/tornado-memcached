tornado-memcached
==============
Asynchronous Memcached client that works within Tornado IO loop.

This is a fork of [yuanwang-1989](https://github.com/yuanwang-1989/tornado_memcache) memcache client modified to own more features and connection pooling support.

Tornado-Memcached is licensed under the Apache Licence, Version 2.0 (http://www.apache.org/licenses/LICENSE-2.0.html).


Usage
-----
```python
import tornado.gen
from tornmc.client import Client

...

client = Client(['127.0.0.1:11211'])

...

@tornado.gen.coroutine
def get(self):
    yield client.set('k', 'v', 5)
    value = yield client.get('k')
```
