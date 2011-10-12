import gevent
import gevent.monkey; gevent.monkey.patch_all()

from collections import defaultdict
import unittest
import datetime
import random

import zookeeper

from zkproto.fixture import ZKProtoFixture
from zkproto.zookeeper_gevent import ZookeeperClient

ZK_OPEN_ACL_UNSAFE = {"perms":zookeeper.PERM_ALL, "scheme":"world", "id" :"anyone"}
ZK_BAD_ACLS = [ZK_OPEN_ACL_UNSAFE]

def p(ts, msg):
    print "%s - %s" % (ts, msg)

class ZookeeperTests(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        self.fixture = ZKProtoFixture({'localhost' : {'url': 'unix://supd.sock'}})
        self.zk = None
        unittest.TestCase.__init__(self, *args, **kwargs)

    def setUp(self):
        if not self.fixture.connected:
            self.fixture.connect_all()

        self.fixture.reset_all_workers()
        self.fixture.start_all_workers()

        self.zk = ZookeeperClient("127.0.0.1:2181", 5000)

    def tearDown(self):
        if self.zk and self.zk.connected:
            self.zk.close()

    def test_kill_the_leader(self):
        def find_the_leader():
            leader = None
            # make sure we see all of the events before we declare a leader
            events = self.fixture.gather_events()
            while leader is None or events:

                for event in events:
                    if event.name == "CONNECTED":
                        p(event.timestamp, "%s connected" % event.source)

                    elif event.name == "LEADER":
                        leader = event
                events = self.fixture.gather_events()
            return leader

        for i in range(3):
            job = gevent.spawn(find_the_leader)
            job.join(15)
            if not job.ready():
                self.fail("Leader didn't emerge after 15 seconds!")
            leader = job.value

            p(leader.timestamp, "%s is the leader!" % leader.source)

            gevent.sleep(random.uniform(0.01, 0.1))

            p(datetime.datetime.now(), "murder most foul!")
            self.fixture.restart_one(leader.supd, leader.source)


    def test_writing(self):
        node="/thewriteplace"

        self.zk.connect()

        self.zk.create(node, "", ZK_BAD_ACLS, zookeeper.EPHEMERAL)

        process_count = self.fixture.process_count

        seen_keys = set()
        writes = defaultdict(int)
        conflicts = defaultdict(int)
        done_count = 0

        print "waiting for %d processes" % process_count

        while done_count < process_count:
            events = self.fixture.gather_events()
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

        data, stat = self.zk.get(node)
        found = defaultdict(int)
        for entry in data.strip().split("\n"):
            found[entry] += 1

        print "\n\nSUMMARY\n=======\n"
        for process in sorted(writes.keys()):
            print "%s\n\twrites: %s\n\tconflicts: %s\n" % (
                process, writes[process], conflicts[process])

        self.assertEqual(len(found), len(writes))
        for k in found:
            self.assertEqual(found[k], writes[k])


def main():
    unittest.main(__name__)

if __name__ == '__main__':
    main()

