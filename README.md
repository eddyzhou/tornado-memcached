tornado-memcache
==============
基于tornado ioloop的异步长连接memcache库

Usage
-----
```python
import tornado.gen
from tornmem.client import Client

...

client = Client(['127.0.0.1:11211'])

...

@tornado.gen.coroutine
def get(self):
    result = yield client.set('k', 'v', 5)
    value = yield client.get('k')
```
