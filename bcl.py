#!/usr/bin/python
from sys import path, argv
path.append("/ssg/bin/python_modules/")
import extraview_cli
from nlog import vlog,die_now
from ClusterShell.NodeSet import NodeSet
from ClusterShell.Task import task_self
import json
import os
import syslog
import pbs
import siblings
import sgi_cluster
import cluster_info
import file_locking
import ib_diagnostics

def initialize_state():
    """ Initialize DATABASE state variable 
    Attempts to load JSON file but will default to clean state table
    """
    global BAD_CABLE_DB, STATE, LOCK

    if not LOCK:
	LOCK = file_locking.try_lock('/var/run/ncar_bcl', tries=10)
	if not LOCK:
	    die_now("unable to obtain lock. please try again later.")
     
    jsonraw = None

    try:
	with open(BAD_CABLE_DB, 'r') as fds:
	    STATE = json.load(fds)
    except Exception as err:
	vlog(1, 'Unable to Open DB: {0}'.format(err))
	STATE = {}

    if len(STATE) == 0:
	vlog(2, 'Initializing new state database')

	STATE = {
		'cables': {}
	}

def release_state():
    """ Releases Database lock and saves """

    global BAD_CABLE_DB, STATE, LOCK

    save_state()

    STATE = None

    if LOCK:
	LOCK.close()
	LOCK = None
	vlog(5, 'released lock')


def save_state():
    """ Save state database to file """
    global BAD_CABLE_DB, STATE, LOCK

    if os.path.isfile(BAD_CABLE_DB):
	os.rename(BAD_CABLE_DB, BAD_CABLE_DB_BACKUP)

    #Save json database of STATE
    with open(BAD_CABLE_DB, 'w') as fds:
	json.dump(STATE, fds, sort_keys=True, indent=4, separators=(',', ': '))

#def add_nodes(nodes, comment, new_state = 'suspect-pending', skip_ev = False):
#    """ Add node to bad node list 
#    list: list of nodes to add to bnl
#    string:: comment as to why added
#    new_state: give state if not already a bad node
#    """
#    global EV
#
#    pbs.set_offline_nodes(nodes, comment)
#
#    for node in nodes:
#	if not node in STATE['nodes']:
#	    STATE['nodes'][node] = {
#		    'extraview': [],  #assigned EV tickets
#		    'siblings': [],   #sibling nodes that need this node down
#		    'comment': comment, #last comment 
#		    'state': new_state  #current state
#		}
#	    vlog(3, 'Added %s' % (node))
#
#	if len(STATE['nodes'][node]['extraview']) < 1 and not skip_ev:
#	    ev_id = EV.create( \
#		    'ssgev', \
#		    'ssg', \
#		    None, \
#		    '%s: Bad Node %s' % (sgi_cluster.get_cluster_name_formal(), node), \
#		    '%s has been added to the %s bad node list.' % (node, sgi_cluster.get_cluster_name_formal()),
#		    {
#			'HELP_LOCATION': EV.get_field_value_to_field_key('HELP_LOCATION', 'NWSC'),
#			'HELP_HOSTNAME': EV.get_field_value_to_field_key('HELP_HOSTNAME', sgi_cluster.get_cluster_name_formal()),
#			'HELP_HOSTNAME_OTHER': node
#		    }
#		) 
#	    if ev_id:
#		STATE['nodes'][node]['extraview'].append(ev_id)
#		vlog(3, 'Opened Extraview Ticket %s for %s' % (ev_id, node))
#
#	for ev_id in STATE['nodes'][node]['extraview']:
#	    EV.add_resolver_comment(ev_id, 'Bad Node Comment:\n%s' % comment)
# 
#    save_state()
#
#def del_nodes(nodes, comment):
#    """ release node from bad node list """
#    global EV
#
#    pbs.set_online_nodes(nodes, comment)
#
#    for node in nodes:
#	if not node in STATE['nodes']:
#	   vlog(3, 'Skipping non bad node %s' % (node))
#	   continue
#       
#        vlog(3, 'Releasing node %s' % (node))
#	for ev_id in STATE['nodes'][node]['extraview']:
#	    EV.close(ev_id, 'Bad Node Comment:\n%s' % comment)
#	    vlog(3, 'Closed Extraview Ticket %s for %s' % (ev_id, node))
#
#	del STATE['nodes'][node]
#	vlog(3, 'Released %s' % (node))
#
#	for snode in STATE['nodes']:
#	    if node in STATE['nodes'][snode]['siblings']: 
#		STATE['nodes'][snode]['siblings'].remove(node)
#		
#		if len(STATE['nodes'][snode]['siblings']) == 0 and (
#			STATE['nodes'][snode]['state'] == 'sibling-pending' or
#			STATE['nodes'][snode]['state'] == 'sibling'):
#			STATE['nodes'][snode]['state'] = 'sibling-released'
# 
#    save_state()
#
#def list_state(nodes):
#    """ dump state to user """
#
#    nodelist = []
#    if nodes != '':
#	nodelist = list(nodes)
#
#    print '{:<20}{:<20}{:<20}{:<20}'.format('Node','state','Extraview','comment')
#    for node,state in STATE['nodes'].iteritems():
#	if len(nodelist) == 0 or node in nodelist:
#	    print '{:<20}{:<20}{:<20}{:<20}'.format(node,state['state'], ','.join(state['extraview']),state['comment'])
# 
#def comment_nodes(nodes, comment):
#    """ add comment to nodes """
#    global EV
#
#    for node in nodes:
#	if not node in STATE['nodes']:
#	   vlog(3, 'skipping not bad node %s' % (node)) 
#	   continue
#       
#	for ev_id in STATE['nodes'][node]['extraview']:
#	    EV.add_resolver_comment(ev_id, 'Bad Node Comment:\n%s' % comment)
#	    vlog(3, '%s EV#%s comment: %s' % (node, ev_id, comment))
#
#	STATE['nodes'][node]['comment'] = comment
# 
#    save_state()
#
#def attach_nodes(nodes, ev_ids):
#    """ add extraview ids to nodes """
#    global EV
#
#    for node in nodes:
#	if not node in STATE['nodes']:
#	   vlog(3, 'skipping not bad node %s' % (node)) 
#	   continue
#       
#	for ev_id in ev_ids:
#	    if not ev_id in STATE['nodes'][node]['extraview']:
#		STATE['nodes'][node]['extraview'].append(ev_id)
#		vlog(3, 'node %s add extraview %s' % (node, ev_id)) 
#
#    save_state()   
#
#def detach_nodes(nodes, ev_ids):
#    """ remove extraview ids to nodes """
#    global EV
#
#    for node in nodes:
#	if not node in STATE['nodes']:
#	   vlog(3, 'skipping not bad node %s' % (node)) 
#	   continue
#       
#	for ev_id in ev_ids:
#	    if ev_id in STATE['nodes'][node]['extraview']:
#		STATE['nodes'][node]['extraview'].remove(ev_id)
#		vlog(3, 'node %s remove extraview %s' % (node, ev_id)) 
#
#    save_state()   
#
#def mark_hardware(nodes, comment):
#    """ Add node to hardware bad node list """
#    global EV
#
#    add_nodes(nodes, comment, 'hardware')
#
#    for node in nodes:
#	if not node in STATE['nodes']:
#	    continue #should not happen
#
#	STATE['nodes'][node]['state'] = 'hardware-pending'
#
#	for ev_id in STATE['nodes'][node]['extraview']:
#	    EV.add_resolver_comment(ev_id, 'Node marked as having hardware issue.\nBad Node Comment:\n%s' % comment)
# 
#	vlog(3, 'node %s marked as bad hardware' % (node)) 
#
#	sibs = siblings.resolve_siblings([node])
#	sibs.remove(node)
#	add_nodes(sibs, 'sibling to %s' % (node), 'sibling-pending', True)
#	for sib in sibs:
#	    if not node in STATE['nodes'][sib]['siblings']:
#		STATE['nodes'][sib]['siblings'].append(node)
#
#		for ev_id in STATE['nodes'][node]['extraview']:
#		    EV.add_resolver_comment(ev_id, 'Sibling node %s added to bad node list' % (sib))
#	 
#    save_state()
#def mark_casg(nodes, comment):
#    """ Add node to casg bad node list """
#    global EV
#
#    mark_hardware(nodes, comment)
#
#    for node in nodes:
#	if not node in STATE['nodes']:
#	    continue #should not happen
#
#	STATE['nodes'][node]['state'] = 'casg-pending'
#        
#    save_state()
#
#def shutdown_node(node):
#    """ shutdown node """
#    task = task_self()
#    task.run('/usr/bin/systemctl poweroff', nodes=node, timeout=60)
#
#def mark_casg_ready(node):
#    """ send node to casg """
#    global EV
#
#    vlog(3, 'shutting down node %s for casg' % (node))
#
#    STATE['nodes'][node]['state'] = 'casg'
#
#    nodetxt=node
#    if len(STATE['nodes'][node]['siblings']):
#	nodetxt='{} (and siblings {})'.format(node, ','.join(STATE['nodes'][node]['siblings']))
#
#    shutdown_node(node)
#
#    for ev_id in STATE['nodes'][node]['extraview']:
#	vlog(3, 'sending extraview %s for node %s to casg' % (ev_id, node))
#	EV.assign_group(ev_id, 'casg', None, {
#	    'COMMENTS': """
#	    Node {} is ready for hardware repairs.
#	    Please ensure node is dark before proceeding with repairs.
#	    """.format(nodetxt)
#	    })
#       
#    save_state() 
#
#def mark_sibling_ready(node):
#    """ sibling node no longer has jobs """
#    vlog(3, 'shutting down sibling node %s' % (node))
#
#    STATE['nodes'][node]['state'] = 'sibling'
#
#    shutdown_node(node)
#
#    save_state()  
#
#def mark_hardware_ready(node):
#    """ hardware ready to be debugged """
#    global EV
#
#    vlog(3, 'shutting down node %s for hardware' % (node))
#
#    STATE['nodes'][node]['state'] = 'hardware'
#
#    shutdown_node(node)
#
#    for ev_id in STATE['nodes'][node]['extraview']:
#	EV.add_resolver_comment(ev_id, 'Node %s powered off' % (node))
#      
#    save_state() 
#
#def run_auto():
#    """ Run auto mode """
#    global EV
#
#    nstates = pbs.node_states()
#    #print json.dumps(nstates, sort_keys=True, indent=4, separators=(',', ': '))
#
#    #make list of nodes that dont have jobs
#    jobless = []
#    for node, nodest in nstates.iteritems():
#        if not ('resources_assigned' in nodest \
#	    and 'ncpus' in nodest['resources_assigned'] \
#	    and nodest['resources_assigned']['ncpus'] > 0
#	    ):
#		jobless.append(node)
#
#    for node, nodest in nstates.iteritems():
#	states = nodest['state'].split(',')
#	known_bad = node in STATE['nodes']
#
#	vlog(5, 'eval node={} state={} jobs={} bad={}'.format(node, nodest['state'], node in jobless, known_bad ))
#
#	#find known bad nodes that are not offline
#	if known_bad and not 'offline' in states:
#	    vlog(2, 'bad node %s was not offline in pbs' % (node))
#	    pbs.set_offline_nodes([node], 'known bad node')
#
#	#look for known bad states
#	if pbs.is_pbs_down(states) and not known_bad:
#	    #node is in bad state but not known to be bad
#	    vlog(2, 'detected node in bad state %s' % (node))
#	    add_nodes([node], 'PBS state = {}'.format(nodest['state']))
#
#	#find nodes in pending states that no longer have jobs
#	if node in jobless and 'offline' in states and known_bad:
#	    has_sibling_job = False
#	    for sib, sibst in STATE['nodes'].iteritems():
#		if node in sibst['siblings'] and not sib in jobless:
#		    has_sibling_job  = True
#		    vlog(5, 'node %s has sibling %s with job' % (node, sib))
#
#	    vlog(5, 'eval pending node={} job_sibling={} state={}'.format(node, has_sibling_job, STATE['nodes'][node]['state']))
#	    if not has_sibling_job:
#		release_pending_node(node)
#	    else:
#		vlog(4, 'bad node %s skipped due to sibling jobs' % (node))
#        
#
#    #find nodes that are powered off and not already bad nodes
#    check_nodes = []
#
#    #create list of nodes that are not bad
#    for node, nodest in nstates.iteritems():
#	if not node in STATE['nodes'] or is_pending_node(node):
#	    check_nodes.append(node)
#
#    vlog(4, 'checking ipmi power status of %s nodes' % (len(check_nodes)))
#    power_status = ipmi.command(NodeSet.fromlist(check_nodes), 'power status')
#    if not power_status:
#	vlog(2, 'unable to call ipmi power status for %s nodes' % (len(check_nodes)))
#    else:
#	for node in check_nodes:
#	    why = False #has value if node is down
#
#	    if node in power_status:
#		if not 'Chassis Power is on' in power_status[node]:
#		    why = 'invalid power status: %s' % (power_status[node])
#	    else:
#		why = 'unable to query power status'
#
#	    #release pending nodes if power is off since pbs won't notice for forever
#	    if node in STATE['nodes']:  
#		if is_pending_node(node) and why:
#		    comment_nodes([node], why)
#		    release_pending_node(node)  
#	    else: #not a bad node yet
#		if why:
#		    add_nodes([node], why)
#		    release_pending_node(node) 
#
#    save_state()
#
#def is_pending_node(node):
#    """ checks node if state is pending """
#
#    state = STATE['nodes'][node]['state']
#    
#    if state == 'casg-pending'		\
#	or state == 'sibling-pending'	\
#	or state == 'casg-pending'	\
#	or state == 'hardware-pending'	\
#	or state == 'suspect-pending':
#	    return True
#    else:
#	return False
# 
#
#def release_pending_node(node):
#    """ Removes pending status from nodes and calls next state """
#
#    state = STATE['nodes'][node]['state']
#    vlog(4, 'bad node %s release pending state %s' % (node, state))
#
#    if state == 'casg-pending':
#	mark_casg_ready(node)
#    if state == 'sibling-pending':
#	mark_sibling_ready(node)
#    if state == 'hardware-pending':
#	mark_hardware_ready(node)
#    if state == 'suspect-pending':
#	STATE['nodes'][node]['state'] = 'suspect' 

def run_parse(dump_dir):
    """ Run parse mode against a dump directory """
    global EV, STATE

#ibdiagnet2.aguid   ibdiagnet2.db_csv  ibdiagnet2.fdbs  ibdiagnet2.lst     ibdiagnet2.net_dump    ibdiagnet2.pkey  ibdiagnet2.sm      ibnetdiscover_cache.log  report.txt
#ibdiagnet2.cables  ibdiagnet2.debug   ibdiagnet2.log   ibdiagnet2.mcfdbs  ibdiagnet2.nodes_info  ibdiagnet2.pm    ibdiag_stdout.txt  ibnetdiscover.log        timestamp.txt

    ports = []
    issues = {'unknown': [], 'counters': [] }

    with open('%s/%s' % (dump_dir,'ibnetdiscover.log') , 'r') as fds:
        ib_diagnostics.parse_ibnetdiscover_cables(ports, fds.read()) 

    with open('%s/%s' % (dump_dir,'ibdiagnet2.log') , 'r') as fds:
	ib_diagnostics.parse_ibdiagnet(ports, issues, fds.read()) 

    vlog(1, str(issues))

#PortPhyState
#2=polling
#3=disabled
#PortState

    ib_diagnostics.parse_ibdiagnet_csv(ports, '%s/ibdiagnet2.db_csv' % (dump_dir))


    ibsp = cluster_info.get_ib_speed()
    ib_diagnostics.find_underperforming_cables ( ports, issues, ibsp['link'], ibsp['width'])

    #initialize_state()
    #STATE['ports'] = ports
    #release_state()
 
 

def dump_help():
    die_now("""NCAR Bad Cable List Multitool

    help: {0}
	Print this help message
 
    list: {0} list
	dump list of all cables in bad cable list
	   
    add: {0} {{node range}} {{add}} {{comment}}
	add node to bad node list 
	open EV against node in SSG queue
	close node in pbs

    release: {0} {{node range}} {{release}} {{comment}}
	remove node from bad node list
	close EV against node
	open node in pbs

    hardware: {0} {{node range}} {{hardware}} {{comment}}
	add node to bad node list and mark as bad hardware
	open EV against node in SSG queue
	close node and siblings in PBS
	when jobs are done:
	    poweroff node

    comment: {0} {{node range}} {{comment}} {{comment}}
	add comment to node's extraview ticket 
	change comment for node in PBS

    casg: {0} {{node range}} {{casg}} {{comment}}
	switch node to hardware bad node list
	when jobs are done:
	    send extraview ticket to CASG
	    poweroff node

    attach: {0} {{node range}} {{attach}} {{extraview ids (comma delimited)}}
	attach comma seperated list of extraview ticket ids to bad nodes

    detach: {0} {{node range}} {{detach}} {{extraview ids (comma delimited)}}
	detach comma seperated list of extraview ticket ids from bad nodes

    parse: {0} {{parse}} {{path to ib dumps dir}}
	todo

    Environment Variables:
	VERBOSE=[1-5]
	    1: lowest
	    5: highest
	    
    """.format(argv[0]))

if not sgi_cluster.is_sac():
    die_now("Only run this on the SAC node")

BAD_CABLE_DB='/etc/ncar_bad_cable_list.json'
BAD_CABLE_DB_BACKUP='/etc/ncar_bad_cable_list.backup.json'
""" const string: Path to JSON database for bad cable list """

STATE={}
""" dictionary: state table of bad cable list
    this is written to the bad cable DB on any changes
"""
LOCK = None

EV = extraview_cli.open_extraview()


vlog(5, argv)

if len(argv) < 2:
    dump_help() 
elif argv[1] == 'parse':
    run_parse(argv[2])  
#elif argv[1] == 'auto':
#    run_auto() 
#elif argv[1] == 'list':
#    NODES=NodeSet('') 
#    list_state(NODES)
#elif len(argv) == 3 and argv[2] == 'list':
#    NODES=NodeSet(argv[1]) 
#    list_state(NODES)
#elif len(argv) == 4:
#    NODES=NodeSet(argv[1]) 
#    CMD=argv[2].lower()
#
#    if CMD == 'add':
#	add_nodes(NODES, argv[3])
#    elif CMD == 'release':
#	del_nodes(NODES, argv[3]) 
#    elif CMD == 'comment':
#	comment_nodes(NODES, argv[3])
#    elif CMD == 'attach':
#	attach_nodes(NODES, argv[3].split(','))
#    elif CMD == 'detach':
#	detach_nodes(NODES, argv[3].split(','))
#    elif CMD == 'hardware':
#	mark_hardware(NODES, argv[3])
#    elif CMD == 'casg':
#	mark_casg(NODES, argv[3]) 
#    else:
#	dump_help() 
else:
    dump_help() 


