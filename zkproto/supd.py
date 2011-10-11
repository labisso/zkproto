from collections import defaultdict
import xmlrpclib

import supervisor.xmlrpc

class Supervisor(object):
    def __init__(self, url, username=None, password=None):
        self.url = url
        self.username = username
        self.password = password

        self.offsets = defaultdict(int)

    def _proxy(self):
        transport = supervisor.xmlrpc.SupervisorTransport(self.username,
                self.password, self.url)
        # using special supervisor.xmlrpc transport so URL here
        # doesn't matter
        return xmlrpclib.ServerProxy('http://127.0.0.1',
                transport=transport)

    def get_group_process_names(self, group):
        procs = self._proxy().supervisor.getAllProcessInfo()
        procnames = []
        for proc in procs:
            if proc['group'] == group:
                procnames.append(group + ":" + proc['name'])
        return procnames

    def get_process_loglines(self, name):
        buf = self._proxy().supervisor.readProcessStdoutLog(name, self.offsets[name], 4096)

        last_newline = buf.rfind('\n')
        if last_newline == -1:
            return []

        read_length = last_newline + 1

        self.offsets[name] += read_length
        return buf[:read_length].split('\n')
    
    def fastforward_log(self, name):
        while self.get_process_loglines(name):
            pass
    
    def restart_process(self, name):
        self._proxy().supervisor.stopProcess(name)
        self._proxy().supervisor.startProcess(name)
    
    def start_group(self, group):
        return self._proxy().supervisor.startProcessGroup(group)

    def stop_group(self, group):
        return self._proxy().supervisor.stopProcessGroup(group)

    def clear_logs(self, process):
        return self._proxy().supervisor.clearProcessLogs(process)

