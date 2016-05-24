#!/usr/bin/env python
# -*- coding: utf-8 -*-


import logging
import socket
import time

from collections import deque
from functools import partial

import tornado.gen
import tornado.ioloop
import tornado.iostream


class PoolExhaustedError(Exception):
    pass


class PoolClosedError(Exception):
    pass


class TimeoutError(Exception):
    pass


class ConnectionTimeoutError(TimeoutError):
    pass


class ReadTimeoutError(TimeoutError):
    pass


class WriteTimeoutError(TimeoutError):
    pass


class Pool(object):

    def __init__(self, host, io_loop, socket_timeout,
                 max_idle=5, max_active=0, idle_timeout=600):
        self.host = host
        self.io_loop = io_loop or tornado.ioloop.IOLoop.current()
        self.socket_timeout = socket_timeout
        self.max_idle = max_idle
        # When zero, there is no limit on the number of connections in the pool
        self.max_active = max_active
        self.idle_timeout = idle_timeout
        self.active = 0
        self.idle_queue = deque()
        self.closed = False

    def active_count(self):
        return self.active

    def put(self, connection):
        if not self.closed:
            connection.idle_at = time.time()
            self.idle_queue.append(connection)
            self.active -= 1
            if len(self.idle_queue) > self.max_idle:
                logging.info('idle connection quantity over max_idle, '
                             'close last one.')
                c = self.idle_queue.popleft()
                c.stream.close()  # close the connection
        else:
            connection.stream.close()

    @tornado.gen.coroutine
    def get_connection(self):
        if self.idle_timeout > 0:
            while len(self.idle_queue) > 0:
                c = self.idle_queue[0]
                if not c:
                    break

                if c.idle_at + self.idle_timeout > time.time():
                    break

                logging.info('idle timeout, prune stale connection.')
                c = self.idle_queue.popleft()
                c.stream.close()  # close the connection

        if self.closed:
            raise PoolClosedError('connection pool closed.')

        if len(self.idle_queue) > 0:
            self.active += 1
            c = self.idle_queue.popleft()
            c.ensure_tcp_timeout()
            raise tornado.gen.Return(c)

        if self.max_active == 0 or self.active < self.max_active:
            logging.info('create new mc connection. now active: %d'
                         % (self.active+1))
            c = PoolConnection(self, self.host, self.io_loop,
                               self.socket_timeout,
                               self.socket_timeout,
                               self.socket_timeout)
            self.active += 1
            c.ensure_tcp_timeout()
            yield c.connect()
            raise tornado.gen.Return(c)
        else:
            raise PoolExhaustedError('connection pool exhausted. active: %d'
                                     % self.active)

    def close(self):
        logging.info('pool close.')
        self.closed = True
        while len(self.idle_queue) > 0:
            c = self.idle_queue.popleft()
            c.disconnect()  # close the connection


class PoolConnection:

    def __init__(self, pool, host, io_loop,
                 connection_timeout, read_timeout, write_timeout):
        self.pool = pool
        self.host = host
        self.io_loop = io_loop or tornado.ioloop.IOLoop.current()
        self.connection_timeout = connection_timeout
        self.read_timeout = read_timeout
        self.write_timeout = write_timeout
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM, 0)
        self.stream = tornado.iostream.IOStream(self.sock)
        self.idle_at = 0
        self.tcp_timeout = None

    @tornado.gen.coroutine
    def connect(self):
        _timeout_handle = self.add_timeout(self.connection_timeout,
                                           error=ConnectionTimeoutError)
        _host, _port = self.host.split(':', 1)
        yield tornado.gen.Task(self.stream.connect, (_host, int(_port)))
        self.remove_timeout(_timeout_handle)

    def ensure_tcp_timeout(self, timeout=60):
        # prevent unclosed tcp connection
        self.tcp_timeout = self.add_timeout(timeout, error=None)

    @tornado.gen.coroutine
    def send_cmd(self, cmd):
        yield self.write(cmd)
        raise tornado.gen.Return()

    @tornado.gen.coroutine
    def write(self, cmd):
        _timeout_handle = self.add_timeout(self.write_timeout,
                                           error=WriteTimeoutError)
        yield tornado.gen.Task(self.stream.write, '%s\r\n' % cmd)
        self.remove_timeout(_timeout_handle)

    @tornado.gen.coroutine
    def read_one_line(self):
        _timeout_handle = self.add_timeout(self.connection_timeout,
                                           error=ReadTimeoutError)
        response = yield tornado.gen.Task(self.stream.read_until, '\r\n')
        self.remove_timeout(_timeout_handle)
        raise tornado.gen.Return(response.strip('\r\n'))

    @tornado.gen.coroutine
    def read_bytes(self, length):
        _timeout_handle = self.add_timeout(self.connection_timeout,
                                           error=ReadTimeoutError)
        response = yield tornado.gen.Task(self.stream.read_bytes, length)
        self.remove_timeout(_timeout_handle)
        raise tornado.gen.Return(response)

    def _on_timeout(self, error=TimeoutError):
        self.stream.close()
        self.pool.active -= 1
        if error is not None:
            raise error()

    def add_timeout(self, seconds, error=TimeoutError):
        return self.io_loop.add_timeout(time.time() + seconds,
                                        partial(self._on_timeout, error=error))

    def remove_timeout(self, timeout_handle):
        if isinstance(timeout_handle, tornado.ioloop._Timeout):
            self.io_loop.remove_timeout(timeout_handle)

    def close(self):
        if self.tcp_timeout:
            self.remove_timeout(self.tcp_timeout)
            self.tcp_timeout = None
        # self.stream.close()
        self.pool.put(self)

    def disconnect(self):
        if self.tcp_timeout:
            self.remove_timeout(self.tcp_timeout)
            self.tcp_timeout = None
        self.stream.close()
        self.pool.active -= 1
