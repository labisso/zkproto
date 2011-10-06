from collections import defaultdict
import xmlrpclib

import supervisor.xmlrpc

class Supervisor(object):
    def __init__(self, url, username=None, password=None):
        self.url = url
        self.username = username
        self.password = password

        self.proxy = None

        self.offsets = defaultdict(int)

    def connect(self):
        transport = supervisor.xmlrpc.SupervisorTransport(self.username,
                self.password, self.url)
        # using special supervisor.xmlrpc transport so URL here
        # doesn't matter
        self.proxy = xmlrpclib.ServerProxy('http://127.0.0.1',
                transport=transport)

    def get_group_process_names(self, group):
        procs = self.proxy.supervisor.getAllProcessInfo()
        procnames = []
        for proc in procs:
            if proc['group'] == group:
                procnames.append(group + ":" + proc['name'])
        return procnames

    def get_process_loglines(self, name):
        buf = self.proxy.supervisor.readProcessStdoutLog(name, self.offsets[name], 4096)

        last_newline = buf.rfind('\n')
        if last_newline == -1:
            return []

        read_length = last_newline + 1

        if read_length != len(buf):
            print "skipping %d bytes of unfinished line!" % (len(buf) - read_length)

        self.offsets[name] += read_length
        return buf[:read_length].split('\n')
    
    def fastforward_log(self, name):
        while self.get_process_loglines(name):
            pass
    
    def restart_process(self, name):
        self.proxy.supervisor.stopProcess(name)
        self.proxy.supervisor.startProcess(name)
    
    def start_group(self, group):
        return self.proxy.supervisor.startProcessGroup(group)

    def stop_group(self, group):
        return self.proxy.supervisor.stopProcessGroup(group)

    def clear_logs(self, process):
        return self.proxy.supervisor.clearProcessLogs(process)

