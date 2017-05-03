#!/usr/bin/env python
from sys import path, argv
path.append("/ssg/bin/python_modules/") 
from nlog import vlog,die_now
import nfile
from ClusterShell.NodeSet import NodeSet
from ClusterShell.Task import task_self
import cluster_info

def exec_opensm_to_string ( cmd, primary_only = False ):
    """ Runs cmd on openSM host and places Return Value, STDOUT, STDERR into returned list  """
    SM = None

    if primary_only:
	SM = NodeSet.fromlist(cluster_info.get_sm()[1:1])
    else:
	SM = NodeSet.fromlist(cluster_info.get_sm())

    output = {}

    task = task_self()

    task.run(
	cmd,
	nodes=SM, 
	timeout=300
    )

    for buffer, nodelist in task.iter_buffers():
	n = str(NodeSet.fromlist(nodelist))

	if not n in output:
	    output[n] = list()

	output[n].append(str(buffer))

    vlog(5, 'exec_opensm_to_string cmd=%s primary_only=%s ret=%s' % (
	[cmd],
	primary_only,
	task.max_retcode()
    ))

    if task.max_retcode() > 0:
	vlog(1, 'Opensm command may have failed with ret code %s: %s' % (task.max_retcode(), cmd))

    return output

def exec_opensm_to_file ( cmd, output_file ):
    """ Runs cmd on openSM host and pipes STDOUT to output_file """

    output = exec_opensm_to_string( cmd, True )

    for node, out in output.iteritems():
	return nfile.write_file(output_file, "\n".join(out))

    return None

def disable_port( guid, port ):
    """ Disable port in fabric 
    Warning: Never disable a port on a HCA. you will have to restart openib on node to re-enable
    GUID must be integer and not hex string
    """

    if not isinstance(guid, (int, long)) or not isinstance(port, (int)):
	vlog(1, 'guid/port must be ints. given %s/P%s %s/%s' % (guid, port, type(guid), type(port)))
	return None

    vlog(2, 'Disabling %s/P%s in fabric' % (hex(guid), port))
    return exec_opensm_to_string('ibportstate -G %s %s disable' % (guid, port))

def enable_port( guid, port ):
    """ Enable port in fabric 
    GUID must be integer and not hex string
    """

    if not isinstance(guid, (int, long)) or not isinstance(port, (int)):
	vlog(1, 'guid/port must be ints. given %s/P%s %s/%s' % (guid, port, type(guid), type(port)))
	return None

    vlog(2, 'Enabling %s/P%s in fabric' % (hex(guid), port))
    return exec_opensm_to_string('ibportstate -G %s %s enable' % (guid, port))

def query_port( guid, port ):
    """ Query port in fabric 
    GUID must be integer and not hex string
    """

    if not isinstance(guid, (int, long)) or not isinstance(port, (int)):
	vlog(1, 'guid/port must be ints. given %s/P%s %s/%s' % (guid, port, type(guid), type(port)))
	return None

    vlog(4, 'Querying %s/P%s in fabric' % (hex(guid), port))
    return exec_opensm_to_string('ibportstate -G %s %s' % (guid, port))

 
