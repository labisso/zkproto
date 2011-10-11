#!/usr/bin/env python

import fcntl
import os

import zookeeper
import gevent
import gevent.event


# this client is inspired by the threadpool recipe in the geventutil package:
#   https://bitbucket.org/denis/gevent-playground/src/tip/geventutil/threadpool.py

def _pipe():
    r, w = os.pipe()
    fcntl.fcntl(r, fcntl.F_SETFL, os.O_NONBLOCK)
    fcntl.fcntl(w, fcntl.F_SETFL, os.O_NONBLOCK)
    return r, w

#noinspection PyUnusedLocal
def _pipe_read_callback(event, eventtype):
    try:
        os.read(event.fd, 1)
    except EnvironmentError:
        pass

class ZooAsyncResult(gevent.event.AsyncResult):
    def __init__(self, pipe):
        self._pipe = pipe
        gevent.event.AsyncResult.__init__(self)

    def set_exception(self, exception):
        gevent.event.AsyncResult.set_exception(self, exception)
        os.write(self._pipe, '\0')

    def set(self, value=None):
        gevent.event.AsyncResult.set(self, value)
        os.write(self._pipe, '\0')

class ZookeeperClient(object):
    """A client to Apache Zookeeper that is friendly to gevent

    TODO lots to do.
    """
    def __init__(self, hosts, timeout):
        self._hosts = hosts
        self._timeout = timeout

        self._pipe_read, self._pipe_write = _pipe()

        self._event = gevent.core.event(
            gevent.core.EV_READ|gevent.core.EV_PERSIST,
            self._pipe_read, _pipe_read_callback
        )
        self._event.add()

        self._connected = False
        self._connected_async_result = self._new_async_result()

    def __del__(self):
        # attempt to clean up the FD from the gevent hub
        if self._event:
            try:
                self._event.cancel()
            except Exception:
                pass

    def _new_async_result(self):
        return ZooAsyncResult(self._pipe_write)

    def _session_watcher(self, handle, type, state, path):
        #TODO fo real
        self._connected = True
        if not self._connected_async_result.ready():
            self._connected_async_result.set()

    def connect(self):
        #TODO connect timeout? async version?
        self._handle = zookeeper.init(self._hosts, self._session_watcher,
                                      self._timeout)
        self._connected_async_result.wait()

    def create_async(self, path, value, acl, flags):
        async_result = self._new_async_result()

        def callback(handle, code, path):
            if code != zookeeper.OK:
                exc = err_to_exception(code)
                async_result.set_exception(exc())
            else:
                async_result.set(path)

        zookeeper.acreate(self._handle, path, value, acl, flags, callback)
        return async_result

    def create(self, path, value, acl, flags):
        async_result = self.create_async(path, value, acl, flags)
        return async_result.get()

    def get_async(self, path, watcher=None):
        async_result = self._new_async_result()

        def callback(handle, code, value, stat):
            if code != zookeeper.OK:
                exc = err_to_exception(code)
                async_result.set_exception(exc)
            else:
                async_result.set((value, stat))

        watcher_callback, watcher_greenlet = self._setup_watcher(watcher)

        #TODO cleanup the watcher greenlet on error

        zookeeper.aget(self._handle, path, watcher_callback, callback)
        return async_result

    def get(self, path, watcher=None):
        async_result = self.get_async(path, watcher)
        return async_result.get()

    def get_children_async(self, path, watcher=None):
        async_result = self._new_async_result()

        def callback(handle, code, children):
            if code != zookeeper.OK:
                exc = err_to_exception(code)
                async_result.set_exception(exc)
            else:
                async_result.set(children)

        watcher_callback, watcher_greenlet = self._setup_watcher(watcher)

        #TODO cleanup the watcher greenlet on error

        zookeeper.aget_children(self._handle, path, watcher_callback, callback)
        return async_result

    def get_children(self, path, watcher=None):
        async_result = self.get_children_async(path, watcher)
        return async_result.get()

    def set_async(self, path, data, version=-1):
        async_result = self._new_async_result()

        def callback(handle, code, stat):
            if code != zookeeper.OK:
                exc = err_to_exception(code)
                async_result.set_exception(exc)
            else:
                async_result.set(stat)

        zookeeper.aset(self._handle, path, data, version, callback)
        return async_result

    def set(self, path, data, version=-1):
        async_result = self.set_async(path, data, version)
        return async_result.get()

    def _setup_watcher(self, fun):
        if fun is None:
            return None, None

        # create an AsyncResult for this watcher
        async_result = self._new_async_result()

        def callback(handle, *args):
            async_result.set(args)

        greenlet = gevent.spawn(_watcher_greenlet, async_result, fun)

        return callback, greenlet

def _watcher_greenlet(async_result, watcher_fun):
    #wait for the result and feed it into the function
    args = async_result.get()
    watcher_fun(*args)

# this dictionary is a port of err_to_exception() from zkpython zookeeper.c
_ERR_TO_EXCEPTION = {
    zookeeper.SYSTEMERROR: zookeeper.SystemErrorException,
    zookeeper.RUNTIMEINCONSISTENCY: zookeeper.RuntimeInconsistencyException,
    zookeeper.DATAINCONSISTENCY: zookeeper.DataInconsistencyException,
    zookeeper.CONNECTIONLOSS: zookeeper.ConnectionLossException,
    zookeeper.MARSHALLINGERROR: zookeeper.MarshallingErrorException,
    zookeeper.UNIMPLEMENTED: zookeeper.UnimplementedException,
    zookeeper.OPERATIONTIMEOUT: zookeeper.OperationTimeoutException,
    zookeeper.BADARGUMENTS: zookeeper.BadArgumentsException,
    zookeeper.APIERROR: zookeeper.ApiErrorException,
    zookeeper.NONODE: zookeeper.NoNodeException,
    zookeeper.NOAUTH: zookeeper.NoAuthException,
    zookeeper.BADVERSION: zookeeper.BadVersionException,
    zookeeper.NOCHILDRENFOREPHEMERALS: zookeeper.NoChildrenForEphemeralsException,
    zookeeper.NODEEXISTS: zookeeper.NodeExistsException,
    zookeeper.INVALIDACL: zookeeper.InvalidACLException,
    zookeeper.AUTHFAILED: zookeeper.AuthFailedException,
    zookeeper.NOTEMPTY: zookeeper.NotEmptyException,
    zookeeper.SESSIONEXPIRED: zookeeper.SessionExpiredException,
    zookeeper.INVALIDCALLBACK: zookeeper.InvalidCallbackException,
    zookeeper.SESSIONMOVED: zookeeper.SESSIONMOVED,
}

def err_to_exception(error_code):
    exc = _ERR_TO_EXCEPTION.get(error_code)
    if exc is None:

        # double check that it isn't an ok resonse
        if error_code == zookeeper.OK:
            return None

        # otherwise generic exception
        return Exception
    return exc