#!/usr/bin/env python

import datetime
import os
import sys
import threading
import logging

import zookeeper

log = logging.getLogger('zkproto')

def configure_logging():
    procname = os.environ.get('SUPERVISOR_PROCESS_NAME')
    if not procname:
        procname = "zkproto"
    format = procname + " %(asctime)s %(levelname)s - %(message)s"
    logging.basicConfig(level=logging.DEBUG, format=format)


ZK_OPEN_ACL_UNSAFE = {"perms":0x1f, "scheme":"world", "id" :"anyone"}
ZK_BAD_ACLS = [ZK_OPEN_ACL_UNSAFE]

class Contender(object):
    def __init__(self, host, node):
        self.host = host
        self.node = node
        
        self.handle = None
        self.connected = False
        self.leader = False
        self.cv = threading.Condition()
        
    def connect(self):
        self.cv.acquire()
        self.handle = zookeeper.init(self.host, self._watcher, 10000)
        self.cv.wait(10)
        if not self.connected:
            raise Exception("Connection to ZK timed out!")
        self.cv.release()

    def _watcher(self, handle, type, state, path):
        log.debug("ZK global watch: handle=%s type=%s state=%s path=%s",
                handle, type, state, path)

        self.cv.acquire()
        self.connected = True
        self.cv.notify()
        self.cv.release()

    def _assume_leadership(self):
        print "%s I AM THE LEADER!" % datetime.datetime.now()
        sys.stdout.flush()
    
    def vie(self):
        # first everyone tries to create the election node (one at most wins)
        try:
            zookeeper.create(self.handle, self.node, '', ZK_BAD_ACLS, 0)
            log.info("Created node: %s", self.node)
        except zookeeper.NodeExistsException:
            log.info("Node already existed: %s", self.node)

        # now everyone creates an ephemeral sequence node under the election

        realpath = zookeeper.create(self.handle, self.node+"/c_", '',
                ZK_BAD_ACLS, zookeeper.SEQUENCE|zookeeper.EPHEMERAL)
        childname = os.path.basename(realpath)

        c = threading.Condition()
        def _watch_predecessor(handle, type, state, path):
            log.debug("ZK predecessor watch: handle=%s type=%s state=%s path=%s",
                    handle, type, state, path)
            c.acquire()
            c.notify()
            c.release()
        
        # list children and pick out the one just below our own
        while True:
            candidates = sorted(zookeeper.get_children(self.handle, self.node,
                None))
            assert candidates, "election node has no children??"


            index = candidates.index(childname)

            if index == 0:
                self._assume_leadership()
                return

            # set a watch on the child just before us
            predecessor = self.node + "/" + candidates[index-1]
            c.acquire()
            try:
                zookeeper.get(self.handle, predecessor, _watch_predecessor)
            except zookeeper.NoNodeException:
                c.release()
                log.info('failed to set watch because node is gone! %s',
                        predecessor)
                continue

            log.info("Waiting for death watch of %s", predecessor)
            c.wait()

def main():
    configure_logging()
    c = Contender('localhost:2181', '/election2')
    c.connect()
    c.vie()
    while True:
        c.cv.acquire()
        c.cv.wait()


if __name__ == '__main__':
    main()
