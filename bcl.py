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

 	create table if not exists cables (
	    cid INTEGER PRIMARY KEY AUTOINCREMENT,
	    state TEXT,
	    ctime INTEGER,
	    length text,
	    --Serial Number
	    SN text,
	    --Product Number
	    PN text 
	    --State enum - watch, suspect, disabled, sibling, removed
	    state text,
	    comment BLOB,
	    --Number of times that cable has gone into suspect state
	    suspected INTEGER,
	    --Extraview Ticket number
	    ticket INTEGER
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

  	CREATE TABLE IF NOT EXISTS issues (
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
	    ( ? IS NULL or raw = ? ) and
	    source = ? and
	    cid = ?
	LIMIT 1
    ''',(
	issue_type,
	issue,
	raw, raw,
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
    
    #find cable status
    SQL.execute('''
	SELECT 
	    cables.cid,
	    cables.state,
	    cables.suspected,
	    cables.ticket,
	    cp1.flabel as cp1_flabel,
	    cp2.flabel as cp2_flabel
	FROM 
	    cables

	INNER JOIN
	    cable_ports as cp1
	ON
	    cables.cid = ? and
	    cables.cid = cp1.cid

	LEFT OUTER JOIN
	    cable_ports as cp2
	ON
	    cables.cid = cp2.cid and
	    cp1.cpid != cp2.cpid
             
	LIMIT 1
    ''',(
	cid,
    ))

    for row in SQL.fetchall():
	suspected = row['suspected'] + 1
	cname = None
	if row['cp2_flabel']:
	    cname = '%s <--> %s ' % (row['cp1_flabel'],row['cp2_flabel'])
	else:
	    cname = row['cp1_flabel']
	tid = row['ticket'] 
             
	if row['state'] == 'watch':
	    #cable was only being watched. send it to suspect
	    if tid is None: 
		tid = EV.create( 
		    'ssgev',
		    'ssg',
		    'nate',
		    '-----TEST----- %s: Bad Cable %s' % (cluster_info.get_cluster_name_formal(), cname), 
		    '%s has been added to the %s bad cable list.' % (
			cname, 
			cluster_info.get_cluster_name_formal()
		    ), { 
			'HELP_LOCATION': EV.get_field_value_to_field_key('HELP_LOCATION', 'NWSC'),
			'HELP_HOSTNAME': EV.get_field_value_to_field_key(
			    'HELP_HOSTNAME', 
			    cluster_info.get_cluster_name_formal()
			),
			'HELP_HOSTNAME_OTHER': cname
		})
		vlog(3, 'Opened Extraview Ticket %s for bad cable %s' % (tid, cid))

	    SQL.execute('''
		UPDATE
		    cables 
		SET
		    state = 'suspect',
		    suspected = ?,
		    ticket = ?
		WHERE
		    cid = ?
		;''', (
		    suspected,
		    tid,
		    cid
	    ));

	    vlog(3, 'Changed cable %s to suspect state %s times' % (cid, suspected))

	EV.add_resolver_comment(tid, 'Bad Cable Issue:\nType: %s\nIssue: %s\nSource: %s\n%s' % (
	    issue_type, 
	    issue, 
	    source,
	    raw
	))

def list_state(what, list_filter):
    """ dump state to user """

    if what == 'cables':
        f='{0:<10}{1:10}{2:<12}{3:<15}{4:<15}{5:<15}{6:<15}{7:<15}{8:<50}{9:<50}{10:<50}'
 	print f.format(
		"cable_id",
		"state",
		"Suspected#",
		"Ticket",
		"ctime",
		"length",
		"Serial_Number",
		"Product_Number",
		"Comment",
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
		cables.state as state,
		cables.comment as comment,
		cables.suspected as suspected,
		cables.ticket as ticket,
		cp1.flabel as cp1_flabel,
		cp1.plabel as cp1_plabel,
		cp2.flabel as cp2_flabel,
		cp2.plabel as cp2_plabel
	    from 
		cables

	    INNER JOIN
		cable_ports as cp1
	    ON
		cables.cid = cp1.cid and
		( ? IS NULL or cables.state = ? )

	    LEFT OUTER JOIN
		cable_ports as cp2
	    ON
		cables.cid = cp2.cid and
		cp2.cpid != cp1.cpid

	    GROUP BY cables.cid
	''', (
	    list_filter, list_filter
	))

	for row in SQL.fetchall():
	    print f.format(
		    'c%s' % (row['cid']),
		    row['state'],
		    row['suspected'],
		    row['ticket'],
		    row['ctime'],
		    row['length'],
		    row['SN'],
		    row['PN'],
		    row['comment'],
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
		ctime,
		length,
		SN,
		PN,
		state,
		suspected
	    ) VALUES (
		?, ?, ?, ?, ?, ?
	    );''', (
		timestamp,
		gv(port1,'LengthDesc'),
		gv(port1,'SN'),
		gv(port1,'PN'), 
		'watch', #watching all cables by default
		0, #new cables havent been suspected yet
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
	{0} list issues
	    dump list of issues

 	{0} list cables [watch|suspect|disabled|sibling|removed]
	    dump list of cables

  	{0} list ports
	    dump list of cable ports

    add: {0} add {{issue description}} {{c#|S{{guid}}/P{{port}}|cable port label}}
	Note: Use GUID/Port syntax or cable id (c#) to avoid applying to wrong cable
	add cable to bad node list 
	open EV against node in SSG queue

    sibling: {0} sibiling {{(bad cable id) c#}} {{c#|S{{guid}}/P{{port}}|cable port label}}
	mark cable as sibling to bad cable
	disables sibling cable in fabric

    disable: {0} disable {{(bad cable id) c#}}
	disables cable in fabric

    casg: {0} casg {{comment}} {{(bad cable id) c#}}
	disables cable in fabric
	send extraview ticket to CASG

    release: {0} release {{comment}} {{(bad cable id) c#}} 
	enable cable
	set cable state to watch

    rejuvenate: {0} rejuvenate {{comment}} {{(bad cable id) c#}} 
	Note: only use this if the cable has been replaced and it was not autodetected
	release cable
	sets the suspected count back to 0
	disassociate Extraview ticket from Cable

    comment: {0} comment {{comment}} {{(bad cable id) c#}}  
	add comment to bad cable's extraview ticket 

    parse: {0} parse {{path to ib dumps dir}}
	reads the output of ibnetdiscover, ibdiagnet2 and ibcv2
	generates issues against errors found 
	checks if any cable has been replaced (new SN) and will set that cable back to watch state

    Environment Variables:
	VERBOSE=[1-5]
	    1: lowest
	    5: highest
	    
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
    lfilter = None
    if len(argv) == 4:
	lfilter = argv[3]
    list_state(argv[2], lfilter)  
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

