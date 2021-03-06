#!/usr/bin/env python
# -*- coding: utf-8 -*-

import uuid

from tornado.testing import AsyncTestCase
from tornado.testing import gen_test

from tornmc.client import Client, MemcachedKeyError


class ClientTestCase(AsyncTestCase):

    @gen_test
    def test_compress(self):
        client = Client(['127.0.0.1:11211'])
        key = uuid.uuid4().hex
        yield client.add(key, '111111111111', 5, min_compress_len=1)
        res = yield client.get(key)
        self.assertEqual(res, '111111111111')
        yield client.replace(key, '222222222', 5, min_compress_len=1)
        res1 = yield client.get(key)
        self.assertEqual(res1, '222222222')
        key2 = uuid.uuid4().hex
        val2 = {
            'foo': 'bar',
            'test': 'test',
            'ls': [1, 2, 3],
            'd': {'foo': 1, 'bar': 'test'}
        }
        yield client.set(key2, val2, 5, min_compress_len=1)
        res2 = yield client.get(key2)
        self.assertEqual(res2, val2)
        yield client.set_multi({'foo': 'foo', 'bar': 'bar'}, 5,
                               key_prefix='tc_', min_compress_len=1)
        res3 = yield client.get_multi(['foo', 'bar'], key_prefix='tc_')
        self.assertEqual(res3, {'foo': 'foo', 'bar': 'bar'})

    @gen_test
    def test_cas(self):
        client = Client(['127.0.0.1:11211'])
        key = uuid.uuid4().hex
        yield client.set(key, 'foo')
        value, cas_id = yield client.gets(key)
        self.assertEqual(value, 'foo')
        res = yield client.cas(key, cas_id, 'bar', 5)
        self.assertEqual(res, True)

        value, cas_id = yield client.gets(key)
        self.assertEqual(value, 'bar')
        yield client.set(key, 'test', 5)
        res = yield client.cas(key, cas_id, 'test2', 5)
        self.assertEqual(res, False)

    @gen_test
    def test_add(self):
        client = Client(['127.0.0.1:11211'])
        key = uuid.uuid4().hex
        res = yield client.add(key, 'test', 5)
        self.assertEqual(res, True)
        res = yield client.add(key, 'test', 5)
        self.assertEqual(res, False)

    @gen_test
    def test_replace(self):
        client = Client(['127.0.0.1:11211'])
        key = uuid.uuid4().hex
        res1 = yield client.add(key, 'foo', 5)
        self.assertEqual(res1, True)
        res2 = yield client.replace(key, 'bar', 5)
        self.assertEqual(res2, True)
        res3 = yield client.get(key)
        self.assertEqual(res3, 'bar')

    @gen_test
    def test_set(self):
        client = Client(['127.0.0.1:11211'])
        for value in ('abc', u'中国', 5, 5l):
            key = uuid.uuid4().hex
            res = yield client.set(key, value, 5)
            self.assertEqual(res, True)

    @gen_test
    def test_get_not_exist(self):
        key = uuid.uuid4().hex
        client = Client(['127.0.0.1:11211'])
        res = yield client.get(key)
        self.assertEqual(res, None)

    @gen_test
    def test_get_exist(self):
        key = uuid.uuid4().hex
        client = Client(['127.0.0.1:11211'])
        yield client.set(key, 'value', 5)
        res = yield client.get(key)
        self.assertEqual(res, 'value')

    @gen_test
    def test_pickler(self):
        key = uuid.uuid4().hex
        client = Client(['127.0.0.1:11211'])
        yield client.set(key, {'foo': 1, 'bar': {'test': 'test'}})
        res = yield client.get(key)
        self.assertEqual(res, {'foo': 1, 'bar': {'test': 'test'}})

    @gen_test
    def test_set_multi(self):
        client = Client(['127.0.0.1:11211'])
        res = yield client.set_multi({'foo': 1, 'bar': 'bar'}, 5,
                                     key_prefix='test_')
        self.assertEqual(len(res), 0)

    @gen_test
    def test_get_multi_all_exist(self):
        client = Client(['127.0.0.1:11211'])
        yield client.set_multi({'foo': 'foo', 'bar': 'bar'}, key_prefix='t_')
        res = yield client.get_multi(['foo', 'bar'], key_prefix='t_')
        self.assertEqual(res, {'foo': 'foo', 'bar': 'bar'})

    @gen_test
    def test_get_multi_not_all_exist(self):
        client = Client(['127.0.0.1:11211'])
        yield client.set_multi({'foo': 'foo', 'bar': 'bar'}, key_prefix='t_')
        res = yield client.get_multi(['foo', 'bar', 'fail'], key_prefix='t_')
        self.assertEqual(res, {'foo': 'foo', 'bar': 'bar'})

    @gen_test
    def test_set_multi_get(self):
        client = Client(['127.0.0.1:11211'])
        yield client.set_multi({'foo': 'foo', 'bar': 'bar'}, key_prefix='tm_')
        res1 = yield client.get('tm_foo')
        res2 = yield client.get('tm_bar')
        self.assertEqual(res1, 'foo')
        self.assertEqual(res2, 'bar')
        yield client.set('tmd_foo', 'tmd_foo')
        res3 = yield client.get('tmd_foo')
        self.assertEqual(res3, 'tmd_foo')
        yield client.set('tmd_foo1', 'tmd_foo1')
        yield client.set('tmd_foo2', 'tmd_foo2')
        yield client.set('tmd_foo3', 'tmd_foo3')
        res4 = yield client.get('tmd_foo2')
        self.assertEqual(res4, 'tmd_foo2')
        res5 = yield client.get('tmd_foo3')
        self.assertEqual(res5, 'tmd_foo3')

    @gen_test
    def test_incr_not_exist(self):
        key = uuid.uuid4().hex
        client = Client(['127.0.0.1:11211'])
        res = yield client.incr(key, delta=2)
        self.assertEqual(res, None)

    @gen_test
    def test_incr(self):
        key = uuid.uuid4().hex
        value = 2
        delta = 2
        client = Client(['127.0.0.1:11211'])
        yield client.set(key, value, 5)
        res = yield client.incr(key, delta=delta)
        self.assertEqual(res, value+delta)

    @gen_test
    def test_decr_not_exist(self):
        key = uuid.uuid4().hex
        client = Client(['127.0.0.1:11211'])
        res = yield client.decr(key, delta=2)
        self.assertEqual(res, None)

    @gen_test
    def test_decr(self):
        key = uuid.uuid4().hex
        value = 2
        delta = 2
        client = Client(['127.0.0.1:11211'])
        yield client.set(key, value, 5)
        res = yield client.decr(key, delta=delta)
        self.assertEqual(res, value-delta)

    @gen_test
    def test_delete_not_exist(self):
        client = Client(['127.0.0.1:11211'])
        key = uuid.uuid4().hex
        res = yield client.delete(key)
        self.assertEqual(res, True)

    @gen_test
    def test_delete_exist(self):
        client = Client(['127.0.0.1:11211'])
        key = uuid.uuid4().hex
        yield client.set(key, 'value', 5)
        res = yield client.delete(key)
        self.assertEqual(res, True)

    @gen_test
    def test_multi_cmd(self):
        key = uuid.uuid4().hex
        client = Client(['127.0.0.1:11211'])
        yield client.set(key, {'foo': 1, 'bar': {'test': 'test'}})
        res = yield client.get(key)
        self.assertEqual(res, {'foo': 1, 'bar': {'test': 'test'}})
        yield client.delete(key)

        yield client.set('test1', 'test1')
        yield client.set('test2', 'test2')
        res1 = yield client.get('test1')
        self.assertEqual(res1, 'test1')

        yield client.set_multi({'foo': 1, 'bar': 'bar'}, 5,
                               key_prefix='test_')
        yield client.set_multi({'0001': 1, '0002': 2}, 5,
                               key_prefix='test_')
        yield client.delete('test_foo')
        res2 = yield client.get_multi(['foo', 'bar', '0001', '0002'],
                                      key_prefix='test_')
        self.assertEqual(res2, {'0001': 1, '0002': 2, 'bar': 'bar'})

    @gen_test
    def test_exception(self):
        client = Client(['127.0.0.1:11211'])
        val = '😄 😊 😃 😉 😍 😘 😚 😳 😁 '
        with self.assertRaises(MemcachedKeyError):
            yield client.get(u'test')
        yield client.set('test', val)
        res = yield client.get('test')
        self.assertEquals(res, val)
        with self.assertRaises(MemcachedKeyError):
            yield client.get(val)
        for p in client.pools.values():
            self.assertEquals(p.active, 0)


if __name__ == '__main__':
    import unittest
    unittest.main()
