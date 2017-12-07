#!/usr/bin/python
import socket
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
        buf = worker.current_msg
	ns = worker.current_msg
        if self._label:
	    if not self._label in self.output:
		self.output[self._label] = []

	    self.output[self._label].append(buf)

    def ev_hup(self, worker):
        if worker.current_rc > 0:
            vlog(2, "clush: %s: exited with exit code %d" % (worker.current_node, worker.current_rc))

    def ev_timeout(self, worker):
	if worker.current_node:
	    vlog(2, "clush: %s: command timeout" % worker.current_node)
	else:
	    vlog(2, "clush: command timeout")

def command(nodeset, command):
    output = {}

    task = task_self()

    vlog(4,'clush_ipmi: nodeset:%s command:%s' % (nodeset, command))

    if not sgi_cluster.is_sac():
	vlog(1, "only run this from SAC node")
	return False

    for node in nodeset:
	lead = sgi_cluster.get_lead(node)
	if lead:
	    if lead == socket.gethostname():
		cmd = '/usr/diags/bin/bcmd -H {0} {1}'.format(sgi_cluster.get_bmc(node), command)
		vlog(4, 'calling bcmd on localhost: %s' % cmd)
	        task.shell(
		    cmd, 
		    timeout=30,  
		    handler=__OutputHandler(node, output)
		) 
	    else:
		cmd = '/usr/diags/bin/bcmd -H {0} {1}'.format(sgi_cluster.get_bmc(node), command)
		vlog(4, 'calling bcmd on %s: %s' % (lead, cmd))
		task.shell(
		    cmd,
		    nodes=lead, 
		    timeout=30,  
		    handler=__OutputHandler(node, output)
		)

    task.run()

    return output
 
