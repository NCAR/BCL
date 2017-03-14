#!/usr/bin/python
from sys import path, argv
path.append("/ssg/bin/python_modules/")
from nlog import vlog,die_now
from ClusterShell.NodeSet import NodeSet
from ClusterShell.Task import task_self
import ClusterShell
import sgi_cluster
import syslog

class __OutputHandler(ClusterShell.Event.EventHandler):
    output = False

    def __init__(self, label, output):
        self._label = label
	self.output = output
    def ev_read(self, worker):
        ns, buf = worker.last_read()
        if self._label:
	    if not self._label in self.output:
		self.output[self._label] = []

	    self.output[self._label].append(buf)

    def ev_hup(self, worker):
        ns, rc = worker.last_retcode()
        if rc > 0:
            vlog(2, "clush: %s: exited with exit code %d" % (ns, rc))

    def ev_timeout(self, worker):
        vlog(2, "clush: %s: command timeout" % worker.last_node())

def command(nodeset, command):
    output = {}

    task = task_self()

    syslog.syslog('clush_ipmi: nodeset:%s command:%s' % (nodeset, command))

    if not sgi_cluster.is_sac():
	vlog(1, "only run this from SAC node")
	return False

    for node in nodeset:
	lead = sgi_cluster.get_lead(node)
	if lead:
	    task.shell(
		'/usr/diags/bin/bcmd -H {} {}'.format(sgi_cluster.get_bmc(node), command), 
		nodes=lead, 
		timeout=30,  
		handler=__OutputHandler(node, output)
	    )

    task.run()

    return output
 
