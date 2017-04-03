#!/usr/bin/python
from sys import path, argv
path.append("/ssg/bin/python_modules/")
import extraview_cli
from nlog import vlog,die_now
from ClusterShell.NodeSet import NodeSet
from ClusterShell.Task import task_self
import yaml
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
import sqlite3

def lock():
    """ Get lock for changes """
    global LOCK

    if not LOCK:
	LOCK = file_locking.try_lock('/var/run/ncar_bcl', tries=10)
	if not LOCK:
	    die_now("unable to obtain lock. please try again later.")
	vlog(5, 'lock obtained')

def unlock():
    """ release lock for changes """
    global LOCK

    if LOCK:
	LOCK.close()
	LOCK = None
	vlog(5, 'released lock')
             


def initialize_db():
    """ Initialize DATABASE state variable 
    Attempts to load Yaml file but will default to clean state table
    """
    global BAD_CABLE_DB, SQL_CONNECTION, SQL

    try:
	SQL_CONNECTION = sqlite3.connect(BAD_CABLE_DB, isolation_level=None)
	SQL_CONNECTION .row_factory = sqlite3.Row
	SQL = SQL_CONNECTION.cursor()
    except Exception as err:
	vlog(1, 'Unable to Open DB: {0}'.format(err))


    lock()
    #SQL.execute('SELECT name FROM sqlite_master WHERE type=? AND name=?', ('table', 'table_name')); 

    #SQL.execute('''                                               
    #SELECT name FROM sqlite_master                                
    #  WHERE type IN ('table','view') AND name NOT LIKE 'sqlite_%' 
    #  UNION ALL                                                   
    #  SELECT name FROM sqlite_temp_master                         
    #    WHERE type IN ('table','view')                            
    #    ORDER BY 1;                                               
    #''')                                                          
    #print SQL.fetchall()                                          

    #print SQL.fetchall()
    SQL.executescript("""
	PRAGMA foreign_keys = ON;
	BEGIN;

	CREATE TABLE IF NOT EXISTS problems (
	    pid INTEGER PRIMARY KEY,
	    state TEXT,
	    comment BLOB,
	    --number of times this problem has re-occurred
	    activations INTEGER
	);

 	create table if not exists cables (
	    cid INTEGER PRIMARY KEY AUTOINCREMENT,
	    state TEXT,
	    ctime INTEGER,
	    length text,
	    --Serial Number
	    SN text,
	    --Product Number
	    PN text 
	);

  	create table if not exists cable_ports (
	    cpid INTEGER PRIMARY KEY AUTOINCREMENT, 
	    cid integer,
	    --Physical Label
	    plabel text,
	    --Firmware Label
	    flabel text,
	    guid text,
	    --PortNum 
	    port integer,
	    --Name (usually hostname)
	    name text,
	    FOREIGN KEY (cid) REFERENCES cables(cid)
	);

	CREATE INDEX IF NOT EXISTS cable_ports_guid_index on cable_ports  (guid, port);

  	create table if not exists issues (
	    iid INTEGER PRIMARY KEY AUTOINCREMENT,
	    -- last time issue was generated
	    mtime integer,
	    -- Parsed error type
	    type text,
	    issue blob,
	    --Raw error message
	    raw blob,
	    --Where error was generated
	    source blob,
	    --cable source of issue (may be null)
	    cid INTEGER,
	    FOREIGN KEY (cid) REFERENCES cables(cid)
	);

 	CREATE TABLE IF NOT EXISTS problem_cables (
	    pid INTEGER,
	    cid INTEGER,
	    FOREIGN KEY (pid) REFERENCES problems(pid)
	    FOREIGN KEY (cid) REFERENCES cables(cid)
	);

  	CREATE TABLE IF NOT EXISTS problem_sibling_cables (
	    pid INTEGER,
	    cid INTEGER,
	    FOREIGN KEY (pid) REFERENCES problems(pid)
	    FOREIGN KEY (cid) REFERENCES cables(cid)
	);

  	CREATE TABLE IF NOT EXISTS problem_issues (
	    pid INTEGER,
	    iid INTEGER,
 	    FOREIGN KEY (pid) REFERENCES problems(pid)
	    FOREIGN KEY (iid) REFERENCES issues(iid) 
	);                       

   	CREATE TABLE IF NOT EXISTS problem_tickets (
	    pid INTEGER,
	    tid INTEGER,
 	    FOREIGN KEY (pid) REFERENCES problems(pid)
	);   

	COMMIT;
    """)
	    
    unlock()

def release_db():
    """ Releases Database """
    global BAD_CABLE_DB, SQL_CONNECTION, SQL

    SQL.close()
    SQL_CONNECTION.close()
    vlog(5, 'released db')

def add_issue(issue_type, cid, issue, raw, source, timestamp):
    """ Add issue to issues list """
    global EV, STATE

    #vlog(3, 'add_issue(%s, %s, %s)' % (ib_diagnostics.port_pretty(port1),ib_diagnostics.port_pretty(port2), issue))

    iid = None

    #find if this exact issue already exists
    SQL.execute('''
	SELECT 
	    iid
	FROM 
	    issues
	WHERE
	    type = ? and
	    issue = ? and
	    raw = ? and
	    source = ? and
	    cid = ?
	LIMIT 1
    ''',(
	issue_type,
	issue,
	raw,
	source,
	cid       
    ))

    #only care about last time this issue was seen	
    #in theory, there should never be more than 1
    #since a new issue would have a new cable
    for row in SQL.fetchall():
	iid = row['iid']
	break

    if not iid:
 	SQL.execute('''
	    INSERT INTO 
	    issues 
	    (
		type,
		issue,
		raw,
		source,
		mtime,
		cid
	    ) VALUES (
		?, ?, ?, ?, ?, ?
	    );''', (
		issue_type,
		issue,
		raw,
		source,
		timestamp,
		cid
	));

	iid = SQL.lastrowid
	vlog(3, 'created new issue i%s type=%s issue=%s cid=%s' % (iid, issue_type, issue, cid))
    else: #issue was found
	#update mtime since we just got a new hit

        SQL.execute('''
	    UPDATE
		issues 
	    SET
		mtime = ?
	    WHERE
		iid = ?
	    ;''', (
		timestamp,
		iid
	));

	vlog(4, 'updated issue i%s mtime' % (iid))


    print iid
 


    return

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
	print 'add %s bool=%s list=%s' % (issue_id,not issue_id in prob['issues'],prob['issues'])
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

    if what == 'cables':
        f='{0:<10}{1:<15}{2:<7}{3:<15}{4:<17}{5:<50}{6:<50}'
 	print f.format(
		"cable_id",
		"ctime",
		"length",
		"Serial_Number",
		"Product_Number",
		"Firmware Label",
		"Physical Label"
	    ) 

 	SQL.execute('''
	    SELECT 
		cables.cid as cid,
		cables.ctime as ctime,
		cables.length as length,
		cables.SN as SN,
		cables.PN as PN,
		cp1.flabel as cp1_flabel,
		cp1.plabel as cp1_plabel,
		cp2.flabel as cp2_flabel,
		cp2.plabel as cp2_plabel
	    from 
		cables

	    INNER JOIN
		cable_ports as cp1
	    ON
		cables.cid = cp1.cid

	    LEFT OUTER JOIN
		cable_ports as cp2
	    ON
		cables.cid = cp2.cid and
		cp2.cpid != cp1.cpid

	    GROUP BY cables.cid
	''')

	for row in SQL.fetchall():
	    print f.format(
		    'c%s' % (row['cid']),
		    row['ctime'],
		    row['length'],
		    row['SN'],
		    row['PN'],
		    '%s <--> %s' % (row['cp1_flabel'], row['cp2_flabel']),
		    '%s <--> %s' % (row['cp1_plabel'], row['cp2_plabel'])
		)

    elif what == 'ports':
        f='{0:<10}{1:<10}{2:<25}{3:<7}{4:<50}{5:<50}{6:<50}'
 	print f.format(
		"cable_id",
		"port_id",
		"guid",
		"port",
		"name",
		"Firmware Label",
		"Physical Label"
	    ) 

 	SQL.execute('''
	    SELECT 
		cid,
		cpid,
		plabel,
		flabel,
		guid,
		port,
		name
	    from 
		cable_ports 
	    ORDER BY cpid ASC
	''')

	for row in SQL.fetchall():
	    print f.format(
		    'c%s' % row['cid'],
		    'p%s' % row['cpid'],
		    hex(int(row['guid'])),
		    row['port'],
		    row['name'],
		    row['flabel'],
		    row['plabel']
		)

    elif what == 'issues':
        f='{0:<10}{1:<10}{2:<15}{3:<20}{4:<50}{5:<50}'
 	print f.format(
		"issue_id",
		"cable_id",
		"mtime",
		"source",
		"issue",
		"raw error"
	    ) 

 	SQL.execute('''
	    SELECT 
		iid,
		type,
		issue,
		raw,
		source,
		mtime,
		cid 
	    from 
		issues 
	    ORDER BY iid ASC
	''')

	for row in SQL.fetchall():
	    print f.format(
		    'i%s' % row['iid'],
		    'c%s' % row['cid'],
		    row['mtime'],
		    row['source'],
		    row['issue'],
		    row['raw'].replace("\n", "\\n") if row['raw'] else None
		)
 
    return 

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
		    cable = STATE['cables'][cid] 
		    fwlabel = '{0} <--> {1}'.format(
			    cable['port1']['plabel'] if cable['port1'] else 'None',
			    cable['port2']['plabel'] if cable['port2'] else 'None'
			)
		    print '{0:>20} c{1}: {2}'.format('Cable',cid, fwlabel)           

	    for iid in prob['issues']:
		issue = STATE['issues'][iid]
		print '{0:>20} i{1}: {2}'.format('Issue',iid, issue['issue'])
 
    release_state()

def convert_guid_intstr(guid):
    """ normalise representation of guid to string of an integer 
	since sqlite cant handle 64bit ints """
    return str(int(guid, 16))

def run_parse(dump_dir):
    """ Run parse mode against a dump directory """
    global EV, STATE, SQL

    def gv(port, key):
	""" get value or none """
	return None if not key in port else port[key]

    def find_cable(port1, port2):
	""" Find (and update) cable in db 
	port1: ib_diagnostics formatted port
	port2: ib_diagnostics formatted port
	"""

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

	#Attempt to find the newest cable by guid/port/SN
	SQL.execute('''
	    SELECT 
		cables.cid as cid
	    from 
		cables

	    INNER JOIN
		cable_ports as cp1
	    ON
		( ? IS NULL or cables.SN = ? ) and
		cables.cid = cp1.cid and
		cp1.guid = ? and
		cp1.port = ?

	    LEFT OUTER JOIN
		cable_ports as cp2
	    ON
		? IS NOT NULL and
		cables.cid = cp2.cid and
		cp2.guid = ? and
		cp2.port = ?    

	    ORDER BY cables.ctime DESC
	    LIMIT 1
	''',(
	    gv(port1, 'SN'), gv(port1, 'SN'),
	    convert_guid_intstr(port1['guid']),
	    int(port1['port']),
	    '1' if port2 else None,
	    convert_guid_intstr(port2['guid']) if port2 else None, 
	    int(port2['port']) if port2 else None,
	))

	rows = SQL.fetchall()
	if len(rows) > 0:
	    #exact cable found
	    return rows[0]['cid']
	else:
	    return None

    def insert_cable(port1, port2, timestamp):
	""" insert new cable into db """

	SQL.execute('''
	    INSERT INTO 
	    cables 
	    (
		state,
		ctime,
		length,
		SN,
		PN
	    ) VALUES (
		?, ?, ?, ?, ?
	    );''', (
		'new', 
		timestamp,
		gv(port1,'LengthDesc'),
		gv(port1,'SN'),
		gv(port1,'PN'), 
	));
	cid = SQL.lastrowid

	#insert the ports
	for port in [port1, port2]:
	    if port:
		SQL.execute('''
		    INSERT INTO cable_ports (
			cid,
			guid,
			name,
			port,
			plabel,
			flabel
		    ) VALUES (
			?, ?, ?, ?, ?, ?
		    );
		''', (
		    cid,
		    convert_guid_intstr(port['guid']),
		    port['name'],
		    int(port['port']),
		    ib_diagnostics.port_pretty(port),
		    ib_diagnostics.port_pretty(port), 
		))
		cpid = SQL.lastrowid

	vlog(5, 'create cable(%s) %s <--> %s' % (cid, ib_diagnostics.port_pretty(port1),ib_diagnostics.port_pretty(port2)))
	return cid

    #ports from ib_diagnostics should not leave this function
    ports = []
    #dict to hold the issues found
    issues = []
    #timestamp to apply to the cables and issues 
    timestamp = time.time()

    with open('%s/%s' % (dump_dir,'timestamp.txt') , 'r') as fds:
	for line in fds:
	    try:
		timestamp = int(line.strip())
	    except:
		pass

    vlog(5, 'parse dir timestamp: %s = %s' % (timestamp, datetime.datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')))

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

    lock()
    SQL.execute('BEGIN;')

    #add every known cable to database
    #slow but keeps sane list of all cables forever for issue tracking
    for port in ports:
	port1 = port
	port2 = port['connection']
        cid = find_cable(port1, port2)

	if not cid:
	    #create the cable
	    cid = insert_cable(port1, port2, timestamp)

	    #find if this cable already existed but with a different field (SN or PN)
	    if gv(port, 'SN') and gv(port, 'PN'):
		SQL.execute('''
		    SELECT 
			cables.cid as cid,
			cables.SN as SN,
			cables.PN as PN,
			cables.ctime as ctime

		    from 
			cables

		    WHERE
			cables.cid != ? and
			cables.SN = ? and
			cables.PN = ?

		    ORDER BY cables.ctime DESC
		    LIMIT 1
		''',(
		    cid,
		    gv(port1, 'SN'),
		    gv(port1, 'PN')
		))

		for row in SQL.fetchall():
		    #TODO: update problem that cable changed
		    vlog(2,'detected cable SN/PN change c%s and c%s from %s/%s to %s/%s' % (
			cid, 
			row['cid'],
			row['SN'],
			row['PN'],
			gv(port1, 'SN'),
			gv(port1, 'PN') 
		    ))

	#record cid in each port to avoid relookup
	port1['cable_id'] = cid
	if port2:
	    port2['cable_id'] = cid

    for issue in issues:
	cid = None

	#set cable from port which was just resolved
	if len(issue['ports']) and 'cable_id' in issue['ports'][0]:
	    cid = issue['ports'][0]['cable_id']

	#hand over cleaned up info for issues
	add_issue(
	    issue['type'],
	    cid,
	    issue['issue'],
	    issue['raw'],
	    issue['source'],
	    timestamp
	)

    SQL.execute('COMMIT;')

    unlock()

def dump_help():
    die_now("""NCAR Bad Cable List Multitool

    help: {0}
	Print this help message
 
    list: 
	{0} list action
	    dump list of problems,cables,issues
	   
	{0} list issues
	    dump list of issues

 	{0} list problems
	    dump list of problems
 
 	{0} list cables
	    dump list of cables

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

BAD_CABLE_DB='/etc/ncar_bad_cable_list.sqlite'
""" const string: Path to JSON database for bad cable list """

LOCK = None

EV = extraview_cli.open_extraview()

initialize_db()

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











release_db()

