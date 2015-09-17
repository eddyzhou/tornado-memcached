#!/usr/bin/env python
# -*- coding: utf-8 -*-

import tornado.gen
import tornado.ioloop
import cPickle as pickle
import collections
import logging
import zlib
from binascii import crc32
from cStringIO import StringIO
from pool import Pool


def cmemcache_hash(key):
    return((((crc32(key) & 0xffffffff) >> 16) & 0x7fff) or 1)

server_hash_function = cmemcache_hash

_FLAG_PICKLE  = 1<<0
_FLAG_INTEGER = 1<<1
_FLAG_LONG    = 1<<2
_FLAG_COMPRESSED = 1 << 3

class Client:

    def __init__(self, hosts, io_loop=None, socket_timeout=5):
        self.hosts = hosts
        self.dead_hosts = hosts
        io_loop = io_loop or tornado.ioloop.IOLoop.instance()
        self.pools = {}
        for host in hosts:
            self.pools[host] = Pool(host, io_loop, socket_timeout)
    
    @tornado.gen.coroutine
    def get(self, key):
        connection = yield self.get_connection(key=key)
        cmd = '%s %s' %('get', key)
        yield connection.send_cmd(cmd)
        head = yield connection.read_one_line()
        if head == 'END':
            connection.close()
            raise tornado.gen.Return(None)
        _, _, flags, length  = head.split(' ')
        length = int(length) + 2 # include \r\n
        flags = int(flags)
        val = yield connection.read_bytes(length)
        end = yield connection.read_one_line()
        assert end == 'END'
        connection.close()

        val = val[:-2] # strip \r\n
        response = self._convert(flags, val)
        raise tornado.gen.Return(response)

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
                logging.error("unpickle failed. err: %s" % e)

        return None

    @tornado.gen.coroutine
    def get_multi(self, keys, key_prefix=''):
        response = {}
        orig_to_noprefix = dict((key_prefix+str(k), k) for k in keys)
        key_dict = self._group_keys(keys, key_prefix)
        for host, key_list in key_dict.iteritems():
            connection = yield self.get_connection(host=host)
            cmd = '%s %s' %('get', " ".join(key_list))
            yield connection.send_cmd(cmd)
            line = yield connection.read_one_line()
            while line and line != 'END':
                _, key, flags, length  = line.split(' ')
                length = int(length) + 2 # include \r\n
                flags = int(flags)
                val = yield connection.read_bytes(length)
                val = val[:-2] # strip \r\n
                response[orig_to_noprefix[key]] = self._convert(flags, val)
                line = yield connection.read_one_line()
            connection.close()
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
        failed_list = []
        for k, value in mapping.iteritems():
            key = key_prefix + str(k)
            flags, value = self.get_store_info(value, min_compress_len)
            connection = yield self.get_connection(key=key)
            cmd = "%s %s %d %d %d\r\n%s" %('set', key, flags, expire, len(value), value)
            yield connection.send_cmd(cmd)
            # read all responses after write all requests may be faster, simple implement right now.
            response = yield connection.read_one_line()
            connection.close()
            if response != 'STORED':
                failed_list.append(k)
        raise tornado.gen.Return(failed_list)

    @tornado.gen.coroutine
    def set(self, key, value, expire=0, min_compress_len=0):
        result = yield self._set("set", key, value, expire, min_compress_len)
        raise tornado.gen.Return(result)

    @tornado.gen.coroutine
    def replace(self, key, value, expire=0, min_compress_len=0):
        result = yield self._set("replace", key, value, expire, min_compress_len)
        raise tornado.gen.Return(result)

    @tornado.gen.coroutine
    def add(self, key, value, expire=0, min_compress_len=0):
        result = yield self._set("add", key, value, expire, min_compress_len)
        raise tornado.gen.Return(result)

    @tornado.gen.coroutine
    def _set(self, cmd, key, value, expire=0, min_compress_len=0):
        flags, value = self.get_store_info(value, min_compress_len)
        connection = yield self.get_connection(key=key)
        cmd = "%s %s %d %d %d\r\n%s" %(cmd, key, flags, expire, len(value), value)
        yield connection.send_cmd(cmd)
        response = yield connection.read_one_line()
        connection.close()
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
            value = "%d" % value
            min_compress_len = 0
        elif isinstance(value, long):
            flags |= _FLAG_LONG
            value = "%d" % value
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
        result = yield self._incr_or_decr('incr', key, delta)
        raise tornado.gen.Return(result)

    @tornado.gen.coroutine
    def decr(self, key, delta=1):
        result = yield self._incr_or_decr('decr', key, delta)
        raise tornado.gen.Return(result)
    
    @tornado.gen.coroutine
    def delete(self, key):
        connection = yield self.get_connection(key=key)
        cmd = 'delete %s' %(key) 
        yield connection.send_cmd(cmd)
        response = yield connection.read_one_line()
        connection.close()
        raise tornado.gen.Return(response in ('DELETED', 'NOT_FOUND'))

    @tornado.gen.coroutine
    def _incr_or_decr(self, cmd, key, delta):
        connection = yield self.get_connection(key=key)
        cmd = '%s %s %d' %(cmd, key, delta) 
        yield connection.send_cmd(cmd)
        response = yield connection.read_one_line()
        if not response.isdigit():
            connection.close()
            raise tornado.gen.Return(None)
        connection.close()
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

    def close(self):
        for _, pool in self.pools.iteritems():
            pool.close()