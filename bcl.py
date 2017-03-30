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
import cluster_info
import file_locking
import ib_diagnostics
import pprint
import time
import datetime

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
		'cables':	{}, #dict of index->cable of all known cables
		'next_cable':	1,  #index of next cable
		'issues':	{}, #list of all issues
		'next_issue':	1,  #index of next issue
		'problems':	{}, #list of all problems
		'next_problem':	1   #index of next problem
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

def find_cable(port1, port2, create = True):
    """ Find (and update) cable in state['cables'] """

    def setup_port(cable_port, port):
	""" add port into to a cable port (just the minimal for later) """
	cable_port['guid'] = port['guid']
	cable_port['port'] = port['port']
	cable_port['LengthDesc'] = port['LengthDesc'] if 'LengthDesc' in port and port['LengthDesc'] else None
	cable_port['SN'] = port['SN'] if 'SN' in port and port['SN'] else None
	cable_port['PN'] = port['PN'] if 'PN' in port and port['PN'] else None 
	cable_port['plabel'] = ib_diagnostics.port_pretty(port)

    def update_ports(cid, port1, port2):
	""" update the entries for ports """

	if port1:
	    if not cable['port1']:
		cable['port1'] = {}
	    setup_port(cable['port1'], port1)
	    
	if port2:
	    if not cable['port2']:
		cable['port2'] = {}
	    setup_port(cable['port2'], port2)

	return cid

    if not port1 and port2:
	port1 = port2
	port2 = None

    if port2 and int(port1['guid'], 16) > int(port2['guid'], 16):
	#always order the ports by largest gid as port 2
	#order doesnt matter as long as it is stable
	port_tmp = port2
	port2 = port1
	port1 = port_tmp

    if not port1:
	return None

    for cid,cable in STATE['cables'].iteritems():
	if int(cable['port1']['guid'],16) == int(port1['guid'],16) and int(cable['port1']['port']) == int(port1['port']):
	    return update_ports(cid, port1, port2)
	if port2 and int(cable['port1']['guid'],16) == int(port2['guid'],16) and int(cable['port1']['port']) == int(port2['port']):
	    return update_ports(cid, port1, port2) 
             	    
    if create:
	#cable ids must be unique for life
	cid = STATE['next_cable']
	STATE['next_cable'] += 1

	vlog(5, 'create cable(%s) %s <--> %s' % (cid, ib_diagnostics.port_pretty(port1),ib_diagnostics.port_pretty(port2)))

	cable = STATE['cables'][cid] = {
		'port1': None,
		'port2': None
	}

	return update_ports(cid, port1, port2) 
    else:
	vlog(5, 'unable to find cable %s <--> %s' % (ib_diagnostics.port_pretty(port1),ib_diagnostics.port_pretty(port2)))
	return None

def add_issue(port1, port2, issue):
    """ Add issue to issues list """
    global EV, STATE

    vlog(3, 'add_issue(%s, %s, %s)' % (ib_diagnostics.port_pretty(port1),ib_diagnostics.port_pretty(port2), issue))

    cissue = None
    cid = None

    if port1 or port2:
	#handle single ports
	if not port1 and port2:
	    port1 = port2
	    port2 = None 

 	#add other port if not included and known
	if not port2 and port1['connection']:
	    port2 = port1['connection']
	    vlog(3, 'resolving cable other port %s <--> %s' % (ib_diagnostics.port_pretty(port1),ib_diagnostics.port_pretty(port2)))
 
	cid = find_cable(port1, port2)

    iid = None
    #check if there is already an existing issue
    for aid, aissue in STATE['issues'].iteritems():
	if aissue['issue'] == issue and aissue['cable'] == cid:
	    cissue = aissue;
	    iid = aid

    if cissue == None:
 	iid = STATE['next_issue']
	STATE['next_issue'] += 1
 
	vlog(2, 'new issue(%s) %s <--> %s' % (iid, ib_diagnostics.port_pretty(port1),ib_diagnostics.port_pretty(port2)))
	cissue = {
	    'cable': cid,
	    'issue': issue,
	    'mtime': None,
	}

	STATE['issues'][iid] = cissue

    cissue['mtime'] = time.time()
    add_problem('New Issue', iid, None)

def add_problem(comment, issue_id = None, new_state = None):
    """ create a problem against an issue """
    global EV, STATE

    vlog(4, 'add_problem(%s, %s, %s)' % (comment, issue_id, new_state))

    #determine if problem already exists for this issue or cable
    pid = None
    prob = None

    if issue_id:
	cid = STATE['issues'][issue_id]['cable']

	for apid,aprob in STATE['problems'].iteritems():
	    if (cid and cid in aprob['cables']) or issue_id in aprob['issues']:
		pid = apid
		prob = aprob
		break

    #add problem against issue
    if not prob:
	pid = STATE['next_problem']
	STATE['next_problem'] += 1

	if new_state is None:
	    new_state = 'new'

	vlog(2, 'new problem(%s) ' % (pid))
	prob = {
	    'state': new_state,
	    'comment': comment,
	    'cables': [],
	    'issues': [],
	    'extraview': []
	}
	
	STATE['problems'][pid] = prob

    #make sure issue and cable are in problem
    if issue_id:
	if not issue_id in prob['issues']:
	    vlog(4, 'adding issue %s to problem %s' % (issue_id, pid))
	    prob['issues'].append(issue_id)

	cid = STATE['issues'][issue_id]['cable']
	if cid and not cid in prob['cables']:
 	    vlog(4, 'adding cable %s to problem %s' % (cid, pid))
	    prob['cables'].append(cid)                      
 

#    if len(cissue['extraview']) < 1 and not skip_ev:
#	ev_id = EV.create( \
#		'ssgev', \
#		'ssg', \
#		'nate', \
#		'-----TEST----- %s: Bad Cable %s' % (cluster_info.get_cluster_name_formal(), cname), \
#		'%s has been added to the %s bad cable list.' % (cname, cluster_info.get_cluster_name_formal()), { \
#		    'HELP_LOCATION': EV.get_field_value_to_field_key('HELP_LOCATION', 'NWSC'),
#		    'HELP_HOSTNAME': EV.get_field_value_to_field_key('HELP_HOSTNAME', cluster_info.get_cluster_name_formal()),
#		    'HELP_HOSTNAME_OTHER': cname
#		})
#
#	if ev_id:
#	     cissue['extraview'].append(ev_id)
#	     vlog(3, 'Opened Extraview Ticket %s for bad cable %s' % (ev_id, cable))
#
#    for ev_id in cissue['extraview']:
#	EV.add_resolver_comment(ev_id, 'Bad Cable Comment:\n%s' % comment)


def list_state(what):
    """ dump state to user """

    initialize_state()

    if what == 'problems':
	f='{0:<10}{1:<10}{2:<20}{3:<20}{4:<20}{5:<50}'
	print f.format("pid","state","extraview","cables","issues","comment")

	for pid,prob in STATE['problems'].iteritems():
	    cables = '-'
	    if len(prob['cables']):
		cables = ','.join(map(lambda x: 'c%s' % (str(x)), prob['cables']))

	    issues = '-'
	    if len(prob['issues']):
		issues = ','.join(map(lambda x: 'i%s' % (str(x)), prob['issues']))

	    print f.format(
		    'p%s' % pid,
		    prob['state'], 
		    ','.join(map(str, prob['extraview'])) if len(prob['extraview']) else '-', 
		    cables,
		    issues,
		    prob['comment']
		)
    elif what == 'issues':
	f='{0:<10}{1:<10}{2:<25}{3:<50}'
 	print f.format("issue_id","cable","last_seen","issue")
	for iid,issue in STATE['issues'].iteritems():
	    print f.format(
		    'i%s' % iid,
		    '-' if not issue['cable'] else 'c%s' % issue['cable'],
		    datetime.datetime.fromtimestamp( int(issue['mtime'])).strftime('%Y-%m-%d %H:%M:%S'),
		    issue['issue']
		)     
    elif what == 'cables':
	f='{0:<10}{1:<7}{2:<15}{3:<17}{4:<50}{5:<50}'
 	print f.format("cable_id","length","Serial_Number","Product_Number","Firmware Label","Physical Label")
	for cid,cable in STATE['cables'].iteritems():
	    fwlabel = '{0} <--> {1}'.format(
		    cable['port1']['plabel'] if cable['port1'] else 'None',
		    cable['port2']['plabel'] if cable['port2'] else 'None'
		)
	    clen = '-'
	    SN = '-'
	    PN = '-'
	    if cable['port1']:
		#cables have same PN/SN/length on both ports
		clen = cable['port1']['LengthDesc'] if cable['port1']['LengthDesc'] else '-'
		SN = cable['port1']['SN'] if cable['port1']['SN'] else '-'
		PN = cable['port1']['PN'] if cable['port1']['PN'] else '-'
	    
	    plabel = fwlabel #TODO# add real -- temp fix --
	    print f.format(
		    'c%s' % cid,
		    clen,
		    SN,
		    PN,
		    fwlabel,
		    plabel
		)     
    elif what == 'action':
	f='{0:<10}{1:<10}{2:<20}{3:<100}'
	print f.format("pid","state","extraview","comment")
	for pid,prob in STATE['problems'].iteritems():
	    print f.format(
		    'p%s' % pid,
		    prob['state'], 
		    ','.join(map(str, prob['extraview'])) if len(prob['extraview']) else '-', 
		    prob['comment']
		)    

 	    for cid in prob['cables']:
		    cable = STATE['cables'][str(cid)] 
		    fwlabel = '{0} <--> {1}'.format(
			    cable['port1']['plabel'] if cable['port1'] else 'None',
			    cable['port2']['plabel'] if cable['port2'] else 'None'
			)

		    print '{0:>20} c{1}: {2}'.format('Cable',cid, fwlabel)           

	    for iid in prob['issues']:
		issue = STATE['issues'][str(iid)]


		print '{0:>20} i{1}: {2}'.format('Issue',iid, issue['issue'])



 
    release_state()
	
#    for node,state in STATE['nodes'].iteritems():
#       if len(nodelist) == 0 or node in nodelist:
#	   print '{:<20}{:<20}{:<20}{:<20}'.format(node,state['state'], ','.join(state['extraview']),state['comment'])
 



#    nodelist = []
#    if nodes != '':
#       nodelist = list(nodes)
#
#    print '{:<20}{:<20}{:<20}{:<20}'.format('Node','state','Extraview','comment')
#    for node,state in STATE['nodes'].iteritems():
#       if len(nodelist) == 0 or node in nodelist:
#	   print '{:<20}{:<20}{:<20}{:<20}'.format(node,state['state'], ','.join(state['extraview']),state['comment'])
 

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

    ports = []
    issues = {'link': [], 'missing': [], 'unexpected': [], 'unknown': [], 'label': [], 'speed': [], 'disabled': [], 'width': [], 'counters': [] }

    with open('%s/%s' % (dump_dir,'ibnetdiscover.log') , 'r') as fds:
        ib_diagnostics.parse_ibnetdiscover_cables(ports, fds.read()) 

    with open('%s/ibdiagnet2.db_csv' % (dump_dir), 'r') as fds:
	ib_diagnostics.parse_ibdiagnet_csv(ports, fds)

    with open('%s/%s' % (dump_dir,'ibdiagnet2.log') , 'r') as fds:
	ib_diagnostics.parse_ibdiagnet(ports, issues, fds.read()) 

    p_ibcv2 = '%s/%s' % (dump_dir,'sgi-ibcv2.log') #optional
    if os.path.isfile(p_ibcv2):
	with open(p_ibcv2, 'r') as fds:
	    ib_diagnostics.parse_sgi_ibcv2(ports, issues, fds.read()) 

    ibsp = cluster_info.get_ib_speed()
    ib_diagnostics.find_underperforming_cables ( ports, issues, ibsp['speed'], ibsp['width'])

    initialize_state()

    #pp = pprint.PrettyPrinter(indent=4)
    #pp.pprint(ports)
 
    for issue in issues['missing']:
	add_issue(issue['port1'], issue['port2'], 'Missing Cable')

    for issue in issues['unexpected']:
	add_cable_issue(issue['port1'], issue['port2'], 'Unexpected Cable')
  
    for issue in issues['unknown']:
	add_issue(None, None, issue)

    for issue in issues['label']:
	add_issue(issue['port'], None, 'Invalid Port Label: %s ' % issue['label'])        

    for issue in issues['counters']:
	add_issue(issue['port'], None, 'Increase in Port Counter: %s=%s ' % (issue['counter'], issue['value']))        

    for issue in issues['link']:
	add_issue(issue['port1'], issue['port2'], 'Link Issue: %s ' % (issue['why']))        
 
    for issue in issues['speed']:
	add_issue(issue['port'], None, 'Invalid Port Speed: %s ' % issue['speed'])        

    for issue in issues['width']:
	add_issue(issue['port'], None, 'Invalid Port Width: %s ' % issue['width'])        
 
    for issue in issues['disabled']:
	add_issue(issue['port'], None, 'Port Physical Layer Disabled')        
                                                                            
 
    pp = pprint.PrettyPrinter(indent=4)
    pp.pprint(STATE)
 
    release_state()

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


Port1 Port2 STATE   EV   Comment Issues
None  None  Suspect 4343 Unknown 
    errors
    errors


	    
    """.format(argv[0]))

if not cluster_info.is_mgr():
    die_now("Only run this on the cluster manager")

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
elif argv[1] == 'list':
    list_state(argv[2])  
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


