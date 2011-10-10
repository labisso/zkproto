#!/usr/bin/env python

import os
import sys
import logging

import gevent
import gevent.event
import zookeeper

from zkproto.zookeeper_gevent import ZookeeperClient
from zkproto import cyvents

log = logging.getLogger('zkproto')
eventlog = logging.getLogger('cyvents')

_procname = os.environ.get('SUPERVISOR_PROCESS_NAME')
if not _procname:
    _procname = "zkproto"

def configure_logging():
    format = _procname + " %(asctime)s %(levelname)s - %(message)s"
    logging.basicConfig(level=logging.DEBUG, format=format)

    eventhandler = logging.StreamHandler(sys.stdout)
    eventlog.addHandler(eventhandler)

def cyvent(name, extra=None):
    cyvents.event(_procname, name, eventlog, extra)

ZK_OPEN_ACL_UNSAFE = {"perms":0x1f, "scheme":"world", "id" :"anyone"}
ZK_BAD_ACLS = [ZK_OPEN_ACL_UNSAFE]

def contender(hosts, node):
    zk = ZookeeperClient(hosts, 10000)

    zk.connect()

    # first everyone tries to create the election node (one at most wins)
    try:
        zk.create(node, "", ZK_BAD_ACLS, 0)
        log.info("Created node: %s", node)
    except zookeeper.NodeExistsException:
        log.info("Node already existed: %s", node)

    # now everyone creates an ephemeral sequence node under the election

    realpath = zk.create(node+"/c_", '', ZK_BAD_ACLS,
                         zookeeper.SEQUENCE | zookeeper.EPHEMERAL)
    childname = os.path.basename(realpath)

    log.debug("Created %s", childname)

    death = gevent.event.Event()
    def death_watch(type, state, path):
        log.debug("ZK predecessor watch: type=%s state=%s path=%s", type,
                  state, path)
        death.set()

    # list children and pick out the one just below our own
    while True:
        candidates = sorted(zk.get_children(node))
        assert candidates, "election node has no children??"

        index = candidates.index(childname)

        if index == 0:
            cyvent("LEADER")

            while True:
                # what else to do here?
                gevent.sleep(10)

        # set a watch on the child just before us
        predecessor = node + "/" + candidates[index-1]
        try:
            zk.get(predecessor, death_watch)
        except zookeeper.NoNodeException:
            log.info('failed to set watch because node is gone! %s',
                    predecessor)
            continue

        cyvent("IN_LINE", extra=dict(order=index))

        log.info("Waiting for death watch of %s", predecessor)
        death.wait()

def main():
    configure_logging()
    c = gevent.spawn(contender, 'localhost:2181', '/election2')
    c.join()


if __name__ == '__main__':
    main()
