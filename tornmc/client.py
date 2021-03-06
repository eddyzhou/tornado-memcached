#!/usr/bin/env python
# -*- coding: utf-8 -*-

import cPickle as pickle
import collections
import logging
import re
import zlib
from binascii import crc32
from cStringIO import StringIO

from pool import Pool

import six

import tornado.gen
import tornado.ioloop


def cmemcache_hash(key):
    return((((crc32(key) & 0xffffffff) >> 16) & 0x7fff) or 1)

server_hash_function = cmemcache_hash

valid_key_chars_re = re.compile(b'[\x21-\x7e\x80-\xff]+$')

_FLAG_PICKLE = 1 << 0
_FLAG_INTEGER = 1 << 1
_FLAG_LONG = 1 << 2
_FLAG_COMPRESSED = 1 << 3


class MemcachedError(Exception):
    pass


class MemcachedKeyError(MemcachedError):
    pass


class MemcachedUnknownCommandError(MemcachedError):
    pass


class MemcachedClientError(MemcachedError):
    pass


class MemcachedServerError(MemcachedError):
    pass


class Client:

    def __init__(self, hosts, io_loop=None, socket_timeout=5,
                 max_connections=10, max_idle=3, idle_timeout=600):
        self.hosts = hosts
        io_loop = io_loop or tornado.ioloop.IOLoop.instance()
        self.pools = {}
        for host in hosts:
            self.pools[host] = Pool(host, io_loop, socket_timeout,
                                    max_idle=max_idle,
                                    max_active=max_connections,
                                    idle_timeout=idle_timeout)

    @tornado.gen.coroutine
    def get(self, key):
        self._check_key(key)
        result = yield self._get('get', key)
        raise tornado.gen.Return(result)

    @tornado.gen.coroutine
    def gets(self, key):
        self._check_key(key)
        result = yield self._get('gets', key)
        raise tornado.gen.Return(result)

    @tornado.gen.coroutine
    def cas(self, key, cas_id, value, expire=0, min_compress_len=0):
        self._check_key(key)
        flags, value = self.get_store_info(value, min_compress_len)
        connection = yield self.get_connection(key=key)
        try:
            cmd = '%s %s %d %d %d %d\r\n%s' % \
                  ('cas', key, flags, expire, len(value), cas_id, value)
            yield connection.send_cmd(cmd)
            response = yield connection.read_one_line()
            self._raise_errors(response, 'cas')
            connection.close()
        except (StandardError, MemcachedError):
            connection.disconnect()
            raise
        raise tornado.gen.Return(response == 'STORED')

    @tornado.gen.coroutine
    def _get(self, cmd, key):
        connection = yield self.get_connection(key=key)
        try:
            command = '%s %s' % (cmd, key)
            yield connection.send_cmd(command)
            head = yield connection.read_one_line()
            self._raise_errors(head, cmd)
            if head == 'END':
                connection.close()
                raise tornado.gen.Return(None)
            if cmd == 'gets':
                _, _, flags, length, cas_id = head.split(' ')
            else:
                _, _, flags, length = head.split(' ')
            length = int(length) + 2  # include \r\n
            flags = int(flags)
            val = yield connection.read_bytes(length)
            end = yield connection.read_one_line()
            assert end == 'END'
            connection.close()
        except (StandardError, MemcachedError):
            connection.disconnect()
            raise
        val = val[:-2]  # strip \r\n
        result = self._convert(flags, val)
        if cmd == 'gets':
            response = (result, int(cas_id))
        else:
            response = result
        raise tornado.gen.Return(response)

    def _check_key(self, key, key_prefix=b''):
        if not isinstance(key, six.binary_type):
            raise MemcachedKeyError('No ascii key: %s' % key)
        key = key_prefix + key
        if not valid_key_chars_re.match(key):
            raise MemcachedKeyError('Key contains invalid character: %s' % key)
        if len(key) > 250:
            raise MemcachedKeyError('Key is too long: %s' % key)

    def _raise_errors(self, line, cmd):
        if line.startswith(b'ERROR'):
            raise MemcachedUnknownCommandError(cmd)

        if line.startswith(b'CLIENT_ERROR'):
            error = line[line.find(b' ') + 1:]
            raise MemcachedClientError(error)

        if line.startswith(b'SERVER_ERROR'):
            error = line[line.find(b' ') + 1:]
            raise MemcachedServerError(error)

    def _convert(self, flags, value):
        if flags & _FLAG_COMPRESSED:
            value = zlib.decompress(value)
            flags &= ~_FLAG_COMPRESSED

        if flags == 0:
            return value
        elif flags & _FLAG_INTEGER:
            return int(value)
        elif flags & _FLAG_LONG:
            return long(value)
        elif flags & _FLAG_PICKLE:
            try:
                file = StringIO(value)
                unpickler = pickle.Unpickler(file)
                return unpickler.load()
            except Exception as e:
                logging.error('unpickle failed. err: %s' % e)

        return None

    @tornado.gen.coroutine
    def get_multi(self, keys, key_prefix=''):
        for key in keys:
            self._check_key(key, key_prefix)

        response = {}
        orig_to_noprefix = dict((key_prefix+str(k), k) for k in keys)
        key_dict = self._group_keys(keys, key_prefix)
        for host, key_list in key_dict.iteritems():
            connection = yield self.get_connection(host=host)
            try:
                cmd = '%s %s' % ('get', ' '.join(key_list))
                yield connection.send_cmd(cmd)
                line = yield connection.read_one_line()
                self._raise_errors(line, 'get')
                while line and line != 'END':
                    _, key, flags, length = line.split(' ')
                    length = int(length) + 2  # include \r\n
                    flags = int(flags)
                    val = yield connection.read_bytes(length)
                    val = val[:-2]  # strip \r\n
                    response[orig_to_noprefix[key]] = self._convert(flags, val)
                    line = yield connection.read_one_line()
                connection.close()
            except (StandardError, MemcachedError):
                connection.disconnect()
                raise
        raise tornado.gen.Return(response)

    def _group_keys(self, keys, key_prefix):
        key_list = [key_prefix + str(k) for k in keys]
        d = collections.defaultdict(list)
        for key in key_list:
            host = self.get_host(key)
            d[host].append(key)
        return d

    @tornado.gen.coroutine
    def set_multi(self, mapping, expire=0, key_prefix='', min_compress_len=0):
        for k in mapping:
            self._check_key(k, key_prefix)

        failed_list = []
        for k, value in mapping.iteritems():
            key = key_prefix + str(k)
            flags, value = self.get_store_info(value, min_compress_len)
            connection = yield self.get_connection(key=key)
            try:
                cmd = '%s %s %d %d %d\r\n%s' \
                      % ('set', key, flags, expire, len(value), value)
                yield connection.send_cmd(cmd)
                # read all responses after write all requests may be faster,
                # simple implement right now.
                response = yield connection.read_one_line()
                self._raise_errors(response, 'set')
                connection.close()
            except (StandardError, MemcachedError):
                connection.disconnect()
                raise
            if response != 'STORED':
                failed_list.append(k)
        raise tornado.gen.Return(failed_list)

    @tornado.gen.coroutine
    def set(self, key, value, expire=0, min_compress_len=0):
        self._check_key(key)
        result = yield self._set('set', key, value, expire, min_compress_len)
        raise tornado.gen.Return(result)

    @tornado.gen.coroutine
    def replace(self, key, value, expire=0, min_compress_len=0):
        self._check_key(key)
        result = yield self._set('replace', key, value,
                                 expire, min_compress_len)
        raise tornado.gen.Return(result)

    @tornado.gen.coroutine
    def add(self, key, value, expire=0, min_compress_len=0):
        self._check_key(key)
        result = yield self._set('add', key, value, expire, min_compress_len)
        raise tornado.gen.Return(result)

    @tornado.gen.coroutine
    def _set(self, cmd, key, value, expire=0, min_compress_len=0):
        flags, value = self.get_store_info(value, min_compress_len)
        connection = yield self.get_connection(key=key)
        try:
            cmd = '%s %s %d %d %d\r\n%s' % \
                  (cmd, key, flags, expire, len(value), value)
            yield connection.send_cmd(cmd)
            response = yield connection.read_one_line()
            self._raise_errors(response, cmd)
            connection.close()
        except (StandardError, MemcachedError):
            connection.disconnect()
            raise
        raise tornado.gen.Return(response == 'STORED')

    def get_store_info(self, value, min_compress_len):
        flags = 0
        if isinstance(value, unicode):
            value = value.encode('utf-8')
            min_compress_len = 0
        elif isinstance(value, str):
            pass
        elif isinstance(value, int):
            flags |= _FLAG_INTEGER
            value = '%d' % value
            min_compress_len = 0
        elif isinstance(value, long):
            flags |= _FLAG_LONG
            value = '%d' % value
        else:
            flags |= _FLAG_PICKLE
            file = StringIO()
            pickler = pickle.Pickler(file)
            pickler.dump(value)
            value = file.getvalue()

        lv = len(value)
        if min_compress_len and lv > min_compress_len:
            comp_val = zlib.compress(value)
            if len(comp_val) < lv:
                flags |= _FLAG_COMPRESSED
                value = comp_val

        return (flags, value)

    @tornado.gen.coroutine
    def incr(self, key, delta=1):
        self._check_key(key)
        result = yield self._incr_or_decr('incr', key, delta)
        raise tornado.gen.Return(result)

    @tornado.gen.coroutine
    def decr(self, key, delta=1):
        self._check_key(key)
        result = yield self._incr_or_decr('decr', key, delta)
        raise tornado.gen.Return(result)

    @tornado.gen.coroutine
    def delete(self, key):
        self._check_key(key)
        connection = yield self.get_connection(key=key)
        try:
            cmd = 'delete %s' % (key)
            yield connection.send_cmd(cmd)
            response = yield connection.read_one_line()
            self._raise_errors(response, 'delete')
            connection.close()
        except (StandardError, MemcachedError):
            connection.disconnect()
            raise
        raise tornado.gen.Return(response in ('DELETED', 'NOT_FOUND'))

    @tornado.gen.coroutine
    def _incr_or_decr(self, cmd, key, delta):
        connection = yield self.get_connection(key=key)
        try:
            cmd = '%s %s %d' % (cmd, key, delta)
            yield connection.send_cmd(cmd)
            response = yield connection.read_one_line()
            self._raise_errors(response, cmd)
            if not response.isdigit():
                connection.close()
                raise tornado.gen.Return(None)
            connection.close()
        except (StandardError, MemcachedError):
            connection.disconnect()
            raise
        raise tornado.gen.Return(int(response))

    def get_host(self, key):
        key_hash = server_hash_function(key)
        return self.hosts[key_hash % len(self.hosts)]

    @tornado.gen.coroutine
    def get_connection(self, key=None, host=None):
        if host is None:
            host = self.get_host(key)
        pool = self.pools[host]
        c = yield pool.get_connection()
        raise tornado.gen.Return(c)

    def disconnect_all(self):
        for _, pool in self.pools.iteritems():
            pool.close()
