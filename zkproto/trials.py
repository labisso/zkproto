from _collections import defaultdict
import gevent
import gevent.monkey; gevent.monkey.patch_all()

import datetime
import random
import time

import zookeeper

from zkproto.fixture import ZKProtoFixture
from zkproto.zookeeper_gevent import ZookeeperClient

ZK_OPEN_ACL_UNSAFE = {"perms":0x1f, "scheme":"world", "id" :"anyone"}
ZK_BAD_ACLS = [ZK_OPEN_ACL_UNSAFE]

def p(ts, msg):
    print "%s - %s" % (ts, msg)

def kill_the_leader(fixture):
    leader = None

    while True:
        while leader is None:
            for event in fixture.gather_events():
                if event.name == "CONNECTED":
                    p(event.timestamp, "%s connected" % event.source)

                elif event.name == "LEADER":
                    leader = event
            time.sleep(0.1)

        p(leader.timestamp, "%s is the leader!" % leader.source)

        time.sleep(random.randint(1,8))

        p(datetime.datetime.now(), "murder most foul!")

        fixture.restart_one(leader.supd, leader.source)
        p(datetime.datetime.now(), "the leader is dead!")

        leader = None


def writing(fixture, node="/thewriteplace"):

    zk = ZookeeperClient("127.0.0.1:2181", 5000)
    zk.connect()

    zk.create(node, "", ZK_BAD_ACLS, zookeeper.EPHEMERAL)

    process_count = fixture.process_count

    seen_keys = set()
    writes = defaultdict(int)
    conflicts = defaultdict(int)
    done_count = 0

    print "waiting for %d processes" % process_count

    while done_count < process_count:
        events = fixture.gather_events()
        for event in events:
            if event.key in seen_keys:
                continue
            seen_keys.add(event.key)

            if event.name == "DONE":
                done_count += 1
            elif event.name == "WRITE_OK":
                writes[event.source] += 1
            elif event.name == "WRITE_CONFLICT":
                conflicts[event.source] += 1
        gevent.sleep(0.3)

    data, stat = zk.get(node)
    found = defaultdict(int)
    for entry in data.strip().split("\n"):
        found[entry] += 1


    print "\n\nSUMMARY\n=======\n"
    for process in sorted(writes.keys()):
        print "%s\n\twrites: %s\n\tconflicts: %s\n" % (process,
                                                       writes[process],
                                                       conflicts[process])


    assert len(found) == len(writes), "%d != %d" % (len(found), len(writes))
    for k in found:
        assert found[k] == writes[k], "%s: claimed=%d found=%d" % (k, writes[k], found[k])

if __name__ == '__main__':
    f = ZKProtoFixture({'localhost' : {'url': 'unix://supd.sock'}})
    f.connect_all()
    f.reset_all_workers()
    f.start_all_workers()

    writing(f)

