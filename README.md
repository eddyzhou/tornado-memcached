tornado-memcached
==============
Asynchronous Memcached client that works within Tornado IO loop.

Developing
-----
Commited code must pass:
* flake8

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

License
-----
Tornado-Memcached is licensed under the Apache Licence, Version 2.0 (http://www.apache.org/licenses/LICENSE-2.0.html).
