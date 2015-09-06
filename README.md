tornado-memcached
==============
Async memcached client base on tornado io-loop

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
    yield client.set('k', 'v', 5)
    value = yield client.get('k')
```
