#!/usr/bin/env python
import random

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
_procgroupname = os.environ.get('SUPERVISOR_GROUP_NAME')
if not _procname:
    _procname = "zkproto"
if _procgroupname:
    _procname = _procgroupname + ":" + _procname

def configure_logging():
    format = _procname + " %(asctime)s %(levelname)s - %(message)s"
    logging.basicConfig(level=logging.DEBUG, format=format)

    eventhandler = logging.StreamHandler(sys.stdout)
    eventlog.addHandler(eventhandler)

def cyvent(name, extra=None):
    cyvents.event(_procname, name, eventlog, extra)

ZK_OPEN_ACL_UNSAFE = {"perms":0x1f, "scheme":"world", "id" :"anyone"}
ZK_BAD_ACLS = [ZK_OPEN_ACL_UNSAFE]

def lead():
    while True:
        # what else to do here?
        gevent.sleep(10)

def contender(zk, node):

    # first everyone tries to create the election node (one at most wins)
    try:
        zk.create(node, "", ZK_BAD_ACLS, 0)
        log.info("Created election node: %s", node)
    except zookeeper.NodeExistsException:
        log.info("Election node already existed: %s", node)

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

            # once elected, lead for life.
            return lead()

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

def cooperative_writer(zk, node):
    """This demonstrates node updates where the old version number is specified.

    If the node has since changed, the update will fail. In this example each
    worker reads the node, adds their name, and writes it back. Afterwards the
    node data can be compared to what each worker claimed to write.

    """
    # wait for the node to exist
    while True:
        try:
            zk.get(node)
        except zookeeper.NoNodeException:
            gevent.sleep(0.1)
        else:
            break

    max_length = 1024 * 32

    # sleep a random amount of time at start
    gevent.sleep(random.uniform(0.1, 1.0))
    while True:

        # grab the existing node data
        data, stat = zk.get(node)

        # add our name to the list
        newdata = data + _procname + "\n"

        # exit if the new data would put the node over its limit
        if len(newdata) > max_length:
            cyvent("DONE")
            return

        # write the new data back to ZK node, careful to specify version

        try:
            zk.set(node, newdata, stat['version'])
        except zookeeper.BadVersionException:
            log.debug("Failed to update data due to version conflict")
            cyvent("WRITE_CONFLICT")
        else:
            log.debug("Wrote our name! len=%d", len(newdata))
            cyvent("WRITE_OK")
            # sleep a random amount of time before next round

            gevent.sleep(random.uniform(0.0, 0.01))


def main():
    configure_logging()

    zk = ZookeeperClient('localhost:2181', 5000)
    zk.connect()

    election_greenlet = gevent.spawn(contender, zk, '/election')

    writer_greenlet = gevent.spawn(cooperative_writer, zk, "/thewriteplace")

    writer_greenlet.join()


if __name__ == '__main__':
    main()
