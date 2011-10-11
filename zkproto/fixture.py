import logging

import gevent

from zkproto import cyvents
from zkproto.supd import Supervisor

log = logging.getLogger(__name__)

class ZKProtoFixture(object):

    def __init__(self, supd_configs, groupname="workers"):

        self.supervisors = {}
        self.groupname = groupname

        self.supd_processes = {}
        
        self.all_processes = []
        for name, supd_config in supd_configs.iteritems():
            url = supd_config['url']
            # TODO username password 
            supd = Supervisor(url)
            self.supervisors[name] = supd

    @property
    def process_count(self):
        return len(self.all_processes)

    def connect_all(self):
        def do_connect(supname, supd):
            log.info("Connecting to supervisor %s", supname)
            procs = supd.get_group_process_names(self.groupname)
            assert procs
            self.supd_processes[supname] = procs

            self.all_processes.extend((supname, proc) for proc in procs)

        jobs = [gevent.spawn(do_connect, supname, supd)
                for supname, supd in self.supervisors.iteritems()]
        gevent.joinall(jobs)

    def reset_all_workers(self):
        def do_reset(supname, supd):
            supd.stop_group(self.groupname)

            subjobs = [gevent.spawn(supd.fastforward_log, procname)
                       for procname in self.supd_processes[supname]]
            gevent.joinall(subjobs)

        jobs = [gevent.spawn(do_reset, supname, supd)
                for supname, supd in self.supervisors.iteritems()]
        gevent.joinall(jobs)

    def start_all_workers(self):
        jobs = [gevent.spawn(supd.start_group, self.groupname)
                for supd in self.supervisors.itervalues()]
        gevent.joinall(jobs)

    def _gather_one(self, supname, procname):
        events = []
        supd = self.supervisors[supname]
        lines = supd.get_process_loglines(procname)
        for line in lines:
            ev = cyvents.event_from_logline(line)
            if ev:
                ev.supd = supname
                ev.source = procname
                events.append(ev)

        return events

    def gather_events(self):
        jobs = []
        for supname in self.supervisors:
            for procname in self.supd_processes[supname]:
                jobs.append(gevent.spawn(self._gather_one, supname, procname))

        gevent.joinall(jobs)
        events = []
        for job in jobs:
            events.extend(job.value)
        events.sort(key=lambda e: e.timestamp)
        return events

    def restart_one(self, supname, procname):
        supd = self.supervisors[supname]
        supd.restart_process(procname)
