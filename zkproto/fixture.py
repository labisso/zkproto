import time
import logging

from zkproto import cyvents
from zkproto.supd import Supervisor

log = logging.getLogger(__name__)

class ZKProtoFixture(object):

    def __init__(self, supd_configs, groupname="workers"):

        self.supervisors = {}
        self.groupname = groupname

        self.supd_processes = {}
        
        for name, supd_config in supd_configs.iteritems():
            url = supd_config['url']
            # TODO username password 
            supd = Supervisor(url)
            self.supervisors[name] = supd

    def connect_all(self):
        for supname, supd in self.supervisors.iteritems():
            log.info("Connecting to supervisor %s", supname)
            supd.connect()

            procs = supd.get_group_process_names(self.groupname)
            assert procs
            self.supd_processes[supname] = procs

    def reset_all_workers(self):
        for supname, supd in self.supervisors.iteritems():
            supd.stop_group(self.groupname)
            for procname in self.supd_processes[supname]:
                supd.fastforward_log(procname)

    def start_all_workers(self):
        for supname, supd in self.supervisors.iteritems():
            supd.start_group(self.groupname)

    def gather_events(self):
        events = []
        for supname, supd in self.supervisors.iteritems():
            for procname in self.supd_processes[supname]:
                lines = supd.get_process_loglines(procname)
                for line in lines:
                    ev = cyvents.event_from_logline(line)
                    if ev:
                        ev.supd = supname
                        ev.source = procname
                        events.append(ev)
        return events

    def restart_one(self, supname, procname):
        supd = self.supervisors[supname]
        supd.restart_process(procname)
