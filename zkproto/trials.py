import datetime
import random
import time

from zkproto.fixture import ZKProtoFixture

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


if __name__ == '__main__':
    f = ZKProtoFixture({'localhost' : {'url': 'unix://supd.sock'}})
    f.connect_all()
    f.reset_all_workers()
    f.start_all_workers()

    kill_the_leader(f)

