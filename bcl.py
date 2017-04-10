#!/usr/bin/python
from sys import path, argv
path.append("/ssg/bin/python_modules/")
import extraview_cli
from nlog import vlog,die_now
from ClusterShell.NodeSet import NodeSet
from ClusterShell.Task import task_self
import os
import syslog
import pbs
import siblings
import cluster_info
import file_locking
import ib_diagnostics
import ib_mgt
import pprint
import time
import datetime
import sqlite3
import re
import csv

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
	    --Sibling cable that this cable is being disabled for currently
	    --only 1 sibling per cable
	    sibling INTEGER,
	    comment BLOB,
	    --Number of times that cable has gone into suspect state
	    suspected INTEGER,
	    --Extraview Ticket number
	    ticket INTEGER,
	    --Physical Label
	    plabel text,
 	    --Firmware Label
	    flabel text,
	    FOREIGN KEY (sibling) REFERENCES cables(cid)
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
	    --if port is an HCA
	    hca BOOLEAN,
	    FOREIGN KEY (cid) REFERENCES cables(cid)
	);

	--Special Table with Vendor firmware to physical label
  	create table if not exists cable_labels (
	    clid INTEGER PRIMARY KEY AUTOINCREMENT,
	    --Physical label override
	    new_plabel TEXT
	);

   	create table if not exists cable_port_labels (
	    cplid INTEGER PRIMARY KEY AUTOINCREMENT,
	    clid INTEGER,
	    --Actual Firmware Label to look for
	    flabel TEXT,
	    --Actual Port Label
	    port INTEGER,
	    --New Physical label to apply
	    new_plabel TEXT,
	    FOREIGN KEY (clid) REFERENCES cable_labels(clid)
	);

	CREATE INDEX IF NOT EXISTS cable_ports_guid_index on cable_ports  (guid, port);

  	CREATE TABLE IF NOT EXISTS issues (
	    iid INTEGER PRIMARY KEY AUTOINCREMENT,
	    -- last time issue was generated
	    mtime integer,
	    -- Parsed error type
	    type text,
	    -- Issue description (aka what is wrong)
	    issue blob,
	    --Raw error message
	    raw blob,
	    --Where error was generated
	    source blob,
	    --cable source of issue (may be null)
	    cid INTEGER,
	    --should these issues be ignored?
	    ignore BOOLEAN,
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

def add_sibling(cid, source_cid, comment):
    """ Add sibling against source_cid """
    global EV

    if not cid or not source_cid:
	vlog(1, 'invalid cid c%s or source cid c%s' % (cid, source_cid))
	return

    source_cid_ticket = None
    source_label = None

    SQL.execute('''
	SELECT 
	    cables.cid,
	    cables.state,
	    cables.ticket,
	    cables.flabel
	FROM 
	    cables
	WHERE
	    cables.cid = ?
	LIMIT 1
    ''',(
	source_cid,
    ))

    for row in SQL.fetchall():
	if row['state'] == 'watch' or row['state'] == 'sibling':
	    vlog(3, 'ignoring sibling cable c%s against c%s as state is %s' % (cid, source_cid, row['state']))
	    return

	source_cid_ticket = row['ticket']
	source_label = row['flabel']

    SQL.execute('''
	SELECT 
	    cables.cid,
	    cables.state as state,
	    cables.ticket,
	    cables.flabel,
	    cp1.guid as cp1_guid,
	    cp1.port as cp1_port,
 	    cp2.guid as cp2_guid,
	    cp2.port as cp2_port
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
	if row['state'] != 'watch':
	    vlog(3, 'ignoring sibling cable c%s from state %s against c%s. only cables in watch state can be a sibling.' % (cid, row['state'],source_cid))
	    return  

	vlog(3, 'setting sibling cable c%s in %s against cable c%s' % (cid, row['state'], source_cid))

	disable_cable_ports(cid)

	SQL.execute('BEGIN;')
	SQL.execute('''
	    UPDATE
		cables 
	    SET
		state = 'sibling',
		sibling = ?,
		comment = ?
	    WHERE
		cid = ?
	    ;''', (
		source_cid,
		comment,
		cid
	));
	SQL.execute('COMMIT;')

	msg =  '''
		Cable %s has been disabled in fabric. Cable marked as sibling to %s.

		Bad Cable Ticket# %s
		Sibling Cable Ticket# %s
	    ''' % (
		row['flabel'],
		source_label,
		source_cid_ticket,
		row['Ticket'],
	    )

	if row['ticket'] and not DISABLE_TICKETS:
	    vlog(3, 'updated Extraview Ticket %s for c%s' % (row['ticket'], cid))
	    EV.add_resolver_comment(row['ticket'], msg)

	if source_cid_ticket and not DISABLE_TICKETS:
	    EV.add_resolver_comment(source_cid_ticket, msg)
            vlog(3, 'updated Extraview Ticket %s for source c%s' % (source_cid_ticket, source_cid))


def add_issue(issue_type, cid, issue, raw, source, timestamp):
    """ Add issue to issues list """
    global EV

    iid = None

    #find if this exact issue already exists
    SQL.execute('''
	SELECT 
	    iid,
	    ignore
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

    SQL.execute('BEGIN;')
    #only care about last time this issue was seen	
    #in theory, there should never be more than 1
    #since a new issue would have a new cable
    for row in SQL.fetchall():
	iid = row['iid']
	if row['ignore'] != 0:
	    vlog(2, 'Ignoring issue i%s' % (iid))
	    return
	break

    if not iid:
 	SQL.execute('''
	    INSERT INTO 
	    issues 
	    (
		ignore,
		type,
		issue,
		raw,
		source,
		mtime,
		cid
	    ) VALUES (
		0, ?, ?, ?, ?, ?, ?
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
	    cables.sibling as sibling,
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
             
	if row['state'] == 'watch' or row['state'] == 'sibling':
	    #cable was only being watched. send it to suspect
	    if tid is None and not DISABLE_TICKETS: 
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
	    elif not DISABLE_TICKETS: 
		EV.assign_group(tid, 'ssg', 'nate', {
		    'COMMENTS':	'''
			Ticket has been reopened for repeat offender bad cable.

			Offense: %s
		    ''' % (suspected)
		});

	    if row['sibling']:
		#suspect cable should not be a sibling
		vlog(3, 'Releasing sibling status of c%s from c%s' % (cid, row['sibling']))

	    SQL.execute('''
		UPDATE
		    cables 
		SET
		    state = 'suspect',
		    suspected = ?,
		    ticket = ?,
		    sibling = NULL
		WHERE
		    cid = ?
		;''', (
		    suspected,
		    tid,
		    cid
	    ));

	    vlog(3, 'Changed cable %s to suspect state %s times' % (cid, suspected))

	if not DISABLE_TICKETS:
	    EV.add_resolver_comment(tid, 'Bad Cable Issue:\nType: %s\nIssue: %s\nSource: %s\n%s' % (
		issue_type, 
		issue, 
		source,
		raw
	    ))

    SQL.execute('COMMIT;')

def resolve_cable(needle):
    """ Resolve user inputed string for cable (needle)

    Honored formats:
	Cable: c#
	Guid/Port: S{guid}/P{port}
	Port Firmware Label
	Port Physical Label
	Port: p#
    """
    global SQL

    cable_match = re.compile(
	r"""
	    ^\s*
	    (?P<needle>
		(?:[sS]|)(?P<guid>(?:0x|)[a-fA-F0-9]*)/P(?P<guidport>[0-9]+)
		|
		c(?P<cid>[0-9]+)
		|
		[pP](?P<cpid>[0-9]+)
		|
		(?P<label_name>(?:\w|-|\ |/)*)/P(?P<label_port>[0-9]+)
		|
		(?P<label>(?:\w|-|\ |/)*(?:\s+<-*>\s+(?:\w|\ |/)*|))
	    )
	    \s*$
	""",
	re.VERBOSE
	) 

    match = cable_match.match(needle)
    if not match:
	return None

    if match.group('cid') or match.group('label') or match.group('label'):
	SQL.execute('''
	    SELECT 
		cables.cid as cid,
		port.cpid as cpid
	    from 
		cables

	    LEFT OUTER JOIN
		cable_ports as port
	    ON
		cables.cid = port.cid 
	    WHERE
		cables.cid = ? or
		cables.plabel = ? or
		cables.flabel = ? 

	    ORDER BY cables.ctime DESC
	    LIMIT 1
	''',(    
	    match.group('cid'),
	    match.group('label'),
	    match.group('label')
	))

	for row in SQL.fetchall():
	    return {'cid':row['cid'], 'cpid':row['cpid']}

    SQL.execute('''
	SELECT 
	    cables.cid as cid,
	    port.cpid as cpid
	from 
	    cables

	INNER JOIN
	    cable_ports as port
	ON
	    (
		port.cpid   = ? or
		port.flabel = ? or
		port.plabel = ? or
		( port.guid = ? and port.port = ? ) or
		( port.name = ? and port.port = ? )
	    ) and
	    cables.cid = port.cid 

	ORDER BY cables.ctime DESC
	LIMIT 1
    ''',(    
	int(match.group('cpid')) if match.group('cpid') else 0,
	match.group('label') if match.group('label') else match.group('needle'),
	match.group('label') if match.group('label') else match.group('needle'),
	convert_guid_intstr(match.group('guid')) if match.group('guid') else None, 
	    int(match.group('guidport')) if match.group('guid') else None,
	match.group('label_name'), match.group('label_port'),
    ))

    for row in SQL.fetchall():
	return {'cid':row['cid'], 'cpid':row['cpid']}

    return None

def resolve_cables(user_input):
    """ Resolve user inputed set of strings into cable id list """

    cids = []

    for needle in user_input:
	cret = resolve_cable(needle)
	vlog(4, 'resolving %s to %s' %(needle, cret))

	if cret and not cret['cid'] in cids:
	    cids.append(cret['cid'])
	else:
	    vlog(2, 'unable to resolve %s to a known cable or port' % (needle))

    return cids

def resolve_issues(user_input):
    """ Resolve user inputed set of strings into issue id list """

    iids = []

    issue_match = re.compile(
	r"""^\s*[iI](?P<iid>[0-9]+)\s*$""",
	re.VERBOSE
	) 

    for needle in user_input:
	match = issue_match.match(needle)
	if not match:
	    vlog(2, 'unable to resolve %s to an issue' % (needle))
	    continue

	iids.append(int(match.group('iid')))

    return iids

def ignore_issue(comment, iid):
    """ Ignore issue """

    SQL.execute('BEGIN;')
    SQL.execute('''
	UPDATE
	    issues 
	SET
	    ignore = 1
	WHERE
	    iid = ?
	;''', (iid,));
    SQL.execute('COMMIT;')

    vlog(2, 'issue i%s will be ignored: %s' % (iid, comment))

def honor_issue(comment, iid):
    """ Honor issue """

    SQL.execute('BEGIN;')
    SQL.execute('''
	UPDATE
	    issues 
	SET
	    ignore = 0
	WHERE
	    iid = ?
	;''', (iid,));
    SQL.execute('COMMIT;')

    vlog(2, 'issue i%s will be honored: %s' % (iid, comment))

def comment_cable(cid, comment):
    """ Add comment to cable """

    SQL.execute('''
	SELECT 
	    cid,
	    ticket,
	    comment
	FROM 
	    cables
	WHERE
	    cables.cid = ?
	LIMIT 1
    ''',(
	cid,
    ))

    SQL.execute('BEGIN;')
    for row in SQL.fetchall():
	vlog(2, 'add comment to cable c%s: %s' % (cid, comment))

	SQL.execute('''
	    UPDATE
		cables 
	    SET
		comment = ?
	    WHERE
		cid = ?
	    ;''', (
		comment,
		cid
	));

	if row['ticket'] and not DISABLE_TICKETS:
	    EV.add_resolver_comment(row['ticket'], 'Bad Cable Comment:\n%s' % comment)
	    vlog(3, 'Updated Extraview Ticket %s for c%s with comment: %s' % (row['ticket'], cid, comment))

    SQL.execute('COMMIT;')

def disable_cable_ports(cid):
    """ Disables cable ports in fabric """

    SQL.execute('''
	SELECT 
            cpid,
	    guid,
	    port,
	    hca
	FROM 
	    cable_ports 
	WHERE
	    cid = ?
    ''',(
	cid,
    ))

    for row in SQL.fetchall(): 
	if row['hca']:
	    vlog(1, 'ignoring request to disable HCA p%s.' % (row['cpid']))
	    continue

	ib_mgt.disable_port(int(row['guid']), int(row['port']))

def enable_cable_ports(cid):
    """ Enables cable ports in fabric """

    SQL.execute('''
	SELECT 
	    guid,
	    port
	FROM 
	    cable_ports 
	WHERE
	    cid = ?
    ''',(
	cid,
    ))

    for row in SQL.fetchall(): 
	ib_mgt.enable_port(int(row['guid']), int(row['port']))

def send_casg(cid, comment):
    """ disable cable and send ticket to CASG """
    disable_cable(cid, comment)

    tid = None
    length = None
    SN = None
    PN = None
    suspected = None
    plabel = None
    flabel = None
    siblings = []

    SQL.execute('''
	SELECT 
	    ticket,
	    length,
	    SN,
	    PN,
	    suspected,
	    plabel,
	    flabel
	FROM 
	    cables
        WHERE
	    cables.cid = ?
	LIMIT 1
    ''',(
	cid,
    ))

    for row in SQL.fetchall(): 
	tid = row['ticket']
	length = row['length']
	SN = row['SN']
	PN = row['PN']
	suspected = row['suspected']
	plabel = row['plabel']
	flabel = row['flabel']

    if not tid:
	vlog(1, 'Cable c%s does not have an associated Extraview Ticket. Refusing to send non-existant ticket to casg' % (cid))
	return False

    #find all siblings
    SQL.execute('''
	SELECT 
	    ticket,
	    length,
	    SN,
	    PN,
	    plabel,
	    flabel
	FROM 
	    cables
        WHERE
	    cables.sibling = ?
	LIMIT 1
    ''',(
	cid,
    ))

    for row in SQL.fetchall(): 
	siblings.append( '''
	    Physical Cable Label: %s
	    Software Cable Label: %s
	    Length: %s
	    Serial: %s
	    Product Number: %s
	    Ticket: %s 
	''' % (
	    row['plabel'] if row['plabel'] else row['flabel'], 
	    row['flabel'],
	    row['length'],
	    row['SN'],
	    row['PN'],
	    row['ticket']
	))

    #EV.assign_group(tid, 'casg', None, {
    if not DISABLE_TICKETS:
	vlog(3, 'Sent Ticket %s to CASG' % (tid))
	EV.assign_group(tid, 'ssg', 'nate', {
	    'COMMENTS':	'''
		--- TEST TICKET: PLEASE RETURN TO SSG ---
		CASG,

		The follow cable has been marked for repairs and has been disabled.
		This cable has had %s events that required repair to date.

		Physical Cable Label: %s
		Software Cable Label: %s
		Length: %s
		Serial: %s
		Product Number: %s

		%s

		The following cables have also been shutdown for this repair work:
		%s

		Please verify that the cable ports are dark before repairing cable or return ticket noting the cables are not disabled.
		If there are any questions or issues, please return this ticket to SSG with details.
	    ''' % (
		    suspected,
		    plabel if plabel else flabel,
		    flabel,
		    length,
		    SN,
		    PN,
		    comment,
		    "\n\n".join(siblings) if siblings  else 'No siblings cables at this time.'
	    )
	});
     

def disable_cable(cid, comment):
    """ disable cable """

    SQL.execute('''
	SELECT 
	    cables.cid,
	    cables.state,
	    cables.suspected,
	    cables.ticket,
	    cables.flabel as flabel
	FROM 
	    cables
        WHERE
	    cables.cid = ?
	LIMIT 1
    ''',(
	cid,
    ))

    for row in SQL.fetchall():
	if row['state'] == 'silbing':
	    vlog(1, 'sibling cable already disabled. ignoring request to disable c%s.' % (cid))
	    return
	elif row['state'] == 'disabled':
 	    vlog(1, 'cable already disabled. ignoring request to disable c%s again.' % (cid))
	    return                    
 	elif row['state'] == 'watch':
 	    vlog(3, 'cable in watch state. adding c%s to bad cable list.' % (cid))
	    add_issue('Manual Entry', cid, comment, 'Disable cable requested. Auto adding cable to bad cables.', 'admin', int(time.time()))

	vlog(3, 'disabling cable c%s.' % (cid))

 	SQL.execute('BEGIN;')
	SQL.execute('''
	    UPDATE
		cables 
	    SET
		state = 'disabled',
		comment = ?
	    WHERE
		cid = ?
	    ;''', (
		comment,
		cid
	));
	SQL.execute('COMMIT;')

	disable_cable_ports(cid)

	if row['ticket'] and not DISABLE_TICKETS:
	    EV.add_resolver_comment(row['ticket'], 'Cable %s disabled.' % (row['flabel']))
	    vlog(3, 'Update Extraview Ticket %s for c%s was disabled' % (row['ticket'], cid))

def release_cable(cid, comment, full = False):
    """ Release cable """

    ticket = None
    flabel = None
    sibling = None

    SQL.execute('''
	SELECT 
	    cables.cid,
	    cables.state,
	    cables.suspected,
	    cables.ticket,
	    cables.flabel as flabel,
	    cables.sibling as sibling,
	    cp1.guid as cp1_guid,
	    cp1.port as cp1_port,
 	    cp2.guid as cp2_guid,
	    cp2.port as cp2_port
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
	if row['state'] == 'watch':
	    vlog(3, 'ignoring release cable c%s from state %s' % (cid, row['state']))
	    return

	vlog(3, 'release cable c%s from state %s' % (cid, row['state']))

        enable_cable_ports(cid)

	SQL.execute('BEGIN;')
	SQL.execute('''
	    UPDATE
		cables 
	    SET
		state = 'watch',
		comment = ?,
		suspected = ?,
		ticket = ?,
		sibling = NULL
	    WHERE
		cid = ?
	    ;''', (
		comment,
		0 if full else row['suspected'],
		None if full else row['ticket'],
		cid
	));
	SQL.execute('COMMIT;')

	ticket = row['ticket']
	flabel = row['flabel']
	sibling = row['sibling']

    #get list of siblings and their tickets
    SQL.execute('''
	SELECT 
	    cid,
	    ticket,
	    flabel
	FROM 
	    cables
	WHERE
	    sibling = ?
    ''',(
	cid,
    ))       

    for row in SQL.fetchall():
	vlog(3, 'release sibling cable c%s of c%s' % (row['cid'], cid))
	release_cable(row['cid'], 'Releasing sibling of %s' % (flabel))

	if ticket and not DISABLE_TICKETS:
	    EV.add_resolver_comment(ticket, 'Sibling cable %s enabled and released.' % row['flabel'])

    #get siblings ticket and tell them sibling was released
    if sibling:
	SQL.execute('''
	    SELECT 
		cid,
		ticket,
		flabel
	    FROM 
		cables
	    WHERE
		cid = ?
	''',(
	    sibling,
	))       

	for row in SQL.fetchall():
	    vlog(3, 'notify cable c%s of sibling release of c%s' % (sibling, cid))

	    if row['ticket'] and not DISABLE_TICKETS:
		EV.add_resolver_comment(row['ticket'], 'Sibling cable %s enabled and released.' % (flabel))
		vlog(3, 'Update Extraview Ticket %s for c%s that c%s was released' % (row['ticket'], sibling, cid))

    if ticket and not DISABLE_TICKETS:
	EV.close(ticket, 'Released Bad Cable\nBad Cable Comment:\n%s' % comment)
	vlog(3, 'Closed Extraview Ticket %s for c%s' % (ticket, cid))


def list_state(what, list_filter):
    """ dump state to user """

    if what == 'cables':
        f='{0:<10}{1:<10}{2:10}{3:<12}{4:<15}{5:<15}{6:<15}{7:<15}{8:<15}{9:<50}{10:<50}{11:<50}'
 	print f.format(
		"cable_id",
		"sibling",
		"state",
		"Suspected#",
		"Ticket",
		"ctime",
		"length",
		"Serial_Number",
		"Product_Number",
		"Comment",
		"Firmware Label (node_desc)",
		"Physical Label"
	    )            
 	SQL.execute('''
	    SELECT 
		cables.cid as cid,
		cables.sibling as scid,
		cables.ctime as ctime,
		cables.length as length,
		cables.SN as SN,
		cables.PN as PN,
		cables.state as state,
		cables.comment as comment,
		cables.suspected as suspected,
		cables.ticket as ticket,
		cables.flabel as flabel,
		cables.plabel as plabel,
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
		    'c%s' % (row['scid']) if row['scid'] else None,
		    row['state'],
		    row['suspected'],
		    row['ticket'],
		    row['ctime'],
		    row['length'] if row['length'] else None,
		    row['SN'] if row['SN'] else None,
		    row['PN'] if row['PN'] else None,
		    row['comment'],
		    row['flabel'],
		    row['plabel']
		)

    elif what == 'ports':
	cid=None
	cpid=None
	if list_filter:
	    b = resolve_cable(list_filter)
	    if b:
		cid = b['cid']
		cpid = b['cpid']
	    else:
		vlog(1, 'unable to resolve %s' % list_filter)
		return

        f='{0:<10}{1:<10}{2:<25}{3:<7}{4:<7}{5:<50}{6:<50}{7:<50}'
 	print f.format(
		"cable_id",
		"port_id",
		"guid",
		"port",
		"HCA",
		"name (node_desc)",
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
		hca,
		name
	    FROM 
		cable_ports 
	    WHERE
		? IS NULL or
		cpid = ? or 
		cid = ?
	    ORDER BY cpid ASC
	''', (
	    0 if cid or cpid else None,
	    cpid,
	    cid
	))

	for row in SQL.fetchall():
	    print f.format(
		    'c%s' % row['cid'],
		    'p%s' % row['cpid'],
		    hex(int(row['guid'])),
		    row['port'],
		    'True' if row['hca'] else 'False',
		    row['name'],
		    row['flabel'],
		    row['plabel']
		)

    elif what == 'issues':
        f='{0:<10}{1:<10}{2:<10}{3:<15}{4:<20}{5:<50}{6:<50}'
 	print f.format(
		"issue_id",
		"cable_id",
		"Ignored",
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
		ignore,
		cid 
	    from 
		issues 
	    ORDER BY iid ASC
	''')

	for row in SQL.fetchall():
	    print f.format(
		    'i%s' % row['iid'],
		    'c%s' % row['cid'],
		    'False' if row['ignore'] == 0 else 'True',
		    row['mtime'],
		    row['source'],
		    row['issue'],
		    row['raw'].replace("\n", "\\n") if row['raw'] else None
		)
    elif what == 'overrides':
        f='{0:<10}{1:<50}{2:<25}{3:<7}{4:<25}{5:<25}{6:<7}{7:<25}'
 	print f.format(
		"id",
		"Physical Label",
		"port1 Firmware Label",
		"port1#",
		"port1 Physical Label",
 		"port2 Firmware Label",
		"port2#",
		"port2 Physical Label"
	    ) 

 	SQL.execute('''
	    SELECT 
		cl.clid as clid,
		cl.new_plabel as cable_plabel,
		p1.flabel as p1_flabel,
		p1.port as p1_port,
		p1.new_plabel as p1_new_plabel,
		p2.flabel as p2_flabel,
		p2.port as p2_port,
		p2.new_plabel as p2_new_plabel
	    FROM 
		cable_labels as cl
	    INNER JOIN
		cable_port_labels as p1
	    ON
		cl.clid = p1.clid
	    INNER JOIN
		cable_port_labels as p2
	    ON
		cl.clid = p2.clid and
		p1.cplid  != p2.cplid 
	    GROUP BY cl.clid
	    ORDER BY cl.clid ASC
	''')

	for row in SQL.fetchall():
	    print f.format(
		    row['clid'],
		    row['cable_plabel'],
		    row['p1_flabel'],
		    row['p1_port'],
		    row['p1_new_plabel'],
 		    row['p2_flabel'],
		    row['p2_port'],
		    row['p2_new_plabel']
		)
 

def convert_guid_intstr(guid):
    """ normalise representation of guid to string of an integer 
	since sqlite cant handle 64bit ints """
    return str(int(guid, 16))

def load_overrides(path_csv):
    """ Loads CSV with label overrides """
    global EV, SQL

    def gv(row, key):
	""" get value or none """
	return None if not key in row else str(row[key])

    count = 0
    SQL.execute('BEGIN;');
    with open(path_csv, 'rb') as csvfile:
	for row in csv.DictReader(csvfile, delimiter=',', quotechar='\"'):
	    SQL.execute('''
		INSERT INTO 
		cable_labels 
		(
		    new_plabel
		) VALUES (
		    ?
		);''', (
		    str(row['cable physical label']),
	    ));

	    clid = SQL.lastrowid
 
	    SQL.executemany('''
		INSERT INTO 
		cable_port_labels  
		(
		    clid,
		    flabel,
		    port,
		    new_plabel
		) VALUES (
		    ?, ?, ?, ?
		);''', [
		    (
			clid,
			gv(row, 'port1 firmware label'),
			gv(row, 'port1 port number'),
			gv(row, 'port1 new physical label')
		    ),
 		    (
			clid,
			gv(row, 'port2 firmware label'),
			gv(row, 'port2 port number'),
			gv(row, 'port2 new physical label')
		    )
		]
	    );

	    count += 1
                                
    SQL.execute('COMMIT;');
    vlog(3, 'loaded %s cable overrides' % (count))

def run_parse(dump_dir):
    """ Run parse mode against a dump directory """
    global EV, SQL

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

	def gpv(port, key):
	    """ get value or none """
	    if not port:
		return None
	    if key == "name":
		return ib_diagnostics.port_name_pretty(port).lower()
	    else:
		return None if not key in port else str(port[key]).lower()

	plabel = None
	port_plabel = { 'port1': None, 'port2': None }

	if port2:
	    #find out if the cable has overrides
	    SQL.execute('''
		SELECT 
		    cl.new_plabel as cable_plabel,
		    p1.new_plabel as p1_new_plabel,
		    p2.new_plabel as p2_new_plabel
		FROM 
		    cable_labels as cl
		INNER JOIN
		    cable_port_labels as p1
		ON
		    cl.clid = p1.clid and
		    lower(p1.flabel) = ? and 
		    p1.port = ?
		INNER JOIN
		    cable_port_labels as p2
		ON
		    cl.clid = p2.clid and
		    p1.cplid != p2.cplid and
		    lower(p2.flabel) = ? and 
		    p2.port = ?             
		LIMIT 1
	    ''', (
		gpv(port1,'name'),
		gpv(port1,'port'),
		gpv(port2,'name'),
		gpv(port2,'port')
	    ))
	    for row in SQL.fetchall():
		plabel = row['cable_plabel']
		port_plabel['port1'] = row['p1_new_plabel']
		port_plabel['port2'] = row['p2_new_plabel']
		vlog(4, 'detected cable label override to %s' % (plabel))

	SQL.execute('''
	    INSERT INTO 
	    cables 
	    (
		ctime,
		length,
		SN,
		PN,
		state,
		suspected,
		flabel,
		plabel
	    ) VALUES (
		?, ?, ?, ?, ?, ?, ?, ?
	    );''', (
		timestamp,
		gv(port1,'LengthDesc'),
		gv(port1,'SN'),
		gv(port1,'PN'), 
		'watch', #watching all cables by default
		0, #new cables havent been suspected yet
		'%s <--> %s' % (ib_diagnostics.port_pretty(port1), ib_diagnostics.port_pretty(port2)),
		plabel
	));
	cid = SQL.lastrowid

	#insert the ports
	for key,port in {'port1': port1, 'port2': port2}.iteritems():
	    if port:
		SQL.execute('''
		    INSERT INTO cable_ports (
			cid,
			guid,
			name,
			port,
			hca,
			plabel,
			flabel
		    ) VALUES (
			?, ?, ?, ?, ?, ?, ?
		    );
		''', (
		    cid,
		    convert_guid_intstr(port['guid']),
		    port['name'],
		    int(port['port']),
		    port['type'] == "CA",
		    port_plabel[key],
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
    known_cables=[] #track every that is found
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

	known_cables.append(cid)

    SQL.execute('COMMIT;')

    #Find any cables that are known but not parsed this time around (aka went dark)
    #ignore any cables in a disabled state
    missing_cables = []
    SQL.execute('''
	    SELECT 
		cid
	    FROM 
		cables
	    WHERE
		state != 'removed' or
		state != 'disabled' or
		state != 'sibling' or
		sibling IS NOT NULL
	''')
    for row in SQL.fetchall():
	if not row['cid'] in known_cables:
	    missing_cables.append(int(row['cid']))

    for cid in missing_cables:
	#Verify missing cables actually matter, ignore single port cables (aka unconnected)
	SQL.execute('''
	    SELECT 
		cables.cid,
		cp1.cpid as cp1_cpid,
		cp2.cpid as cp2_cpid
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
	    ''', (
		cid,
	    ))
	for row in SQL.fetchall():
	    if row['cp1_cpid'] and row['cp2_cpid']:
		#cable went dark
		add_issue(
		    'missing cable',
		    cid,
		    'Cable went missing',
		    None,
		    'ibnetdiscover -p',
		    timestamp
		)              
	    else:
		vlog(3, 'Ignoring missing single port Cable %s' % (cid))

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

    SQL.execute('VACUUM;')

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

  	{0} list ports {{c#|S{{guid}}/P{{port}}|cable port label}} 
	    dump list of cable ports

 	{0} list overrides
	    dump list of cable label overrides

    add: {0} add {{issue description}} {{c#|S{{guid}}/P{{port}}|cable port label}}+
	Note: Use GUID/Port syntax or cable id (c#) to avoid applying to wrong cable
	add cable to bad node list 
	open EV against node in SSG queue or Assign to SSG queue

    sibling: {0} sibiling {{(bad cable id) c#}} {{c#|S{{guid}}/P{{port}}|sibling cable port label}}+
	mark cable as sibling to bad cable if sibling is in watch state
	disables sibling cable in fabric

    disable: {0} disable 'comment' {{(bad cable id) c#}}+
	disables cable in fabric
	add cable to bad cable list (if not one already)

    casg: {0} casg {{comment}} {{(bad cable id) c#}}+
	disables cable in fabric
	send extraview ticket to CASG

    release: {0} release {{comment}} {{(bad cable id) c#}}+
	enable cable in fabric
	set cable state to watch
	close Extraview ticket
	release any sibling cables
	release sibling status of cable (don't consider cable as sibling anymore)

    rejuvenate: {0} rejuvenate {{comment}} {{(bad cable id) c#}}+
	Note: only use this if the cable has been replaced and it was not autodetected
	release cable
	sets the suspected count back to 0
	disassociate Extraview ticket from Cable

    comment: {0} comment {{comment}} {{(bad cable id) c#}}+
	add comment to bad cable's extraview ticket 

    ignore: {0} ignore {{comment}} {{(issue id) i#}}+
	Note: only use this in special cases for issues that can not be fixed
	ignore issue with assigned cable until honor requested

    honor: {0} honor {{comment}} {{(issue id) i#}}+
	removes ignore status of issue
 
    parse: {0} parse {{path to ib dumps dir}}
	reads the output of ibnetdiscover, ibdiagnet2 and ibcv2
	generates issues against errors found 
	checks if any cable has been replaced (new SN) and will set that cable back to watch state
	TODO: detect disabled cables and put them in disabled state
	TODO: detect sibling cable swaps
	TODO: detect new SN

    load_overrides: {0} load_overrides {{path to csv}}
	reads csv with following column labels (first line of CSV):
	    'cable physical label'
	    'port1 firmware label'
	    'port1 port number'
	    'port1 new physical label'
	    'port2 firmware label'
	    'port2 port number'
	    'port2 new physical label'
	any cable discovered will have physical label overrode

    Optional Environment Variables:
	VERBOSE={{1-5 default=3}}
	    1: lowest
	    5: highest
	    
	DISABLE_TICKETS={{YES|NO default=NO}}
	    YES: disable creating and updating tickets (may cause extra errors)
	    NO: create tickets
 
    """.format(argv[0]))

if not cluster_info.is_mgr():
    die_now("Only run this on the cluster manager")

BAD_CABLE_DB='/etc/ncar_bad_cable_list.sqlite'
""" const string: Path to JSON database for bad cable list """

LOCK = None
DISABLE_TICKETS=False
EV = None

if 'DISABLE_TICKETS' in os.environ and os.environ['DISABLE_TICKETS'] == "YES":
    DISABLE_TICKETS=True
    vlog(1, 'Disabling creating of extraview tickets')
else:
    EV = extraview_cli.open_extraview()

initialize_db()

vlog(5, argv)

if len(argv) < 2:
    dump_help() 
else:
    CMD=argv[1].lower()
    if CMD == 'parse':
	run_parse(argv[2])  
    if CMD == 'load_overrides':
	load_overrides(argv[2])   
    elif CMD == 'list':
	lfilter = None
	if len(argv) == 4:
	    lfilter = argv[3]
	list_state(argv[2], lfilter)  
    elif CMD == 'disable':
	for cid in resolve_cables(argv[3:]):
	    disable_cable(cid, argv[2])
    elif CMD == 'casg':
	for cid in resolve_cables(argv[3:]):
	    send_casg(cid, argv[2]) 
    elif CMD == 'add':
	for cid in resolve_cables(argv[3:]):
	    add_issue('Manual Entry', cid, argv[2], None, 'admin', int(time.time()))
    elif CMD == 'release':
	for cid in resolve_cables(argv[3:]):
	    release_cable(cid, argv[2])
    elif CMD == 'rejuvenate':
	for cid in resolve_cables(argv[3:]):
	    release_cable(cid, argv[2], True)
    elif CMD == 'ignore':
	for iid in resolve_issues(argv[3:]):
	    ignore_issue(argv[2], iid)
    elif CMD == 'honor':
	for iid in resolve_issues(argv[3:]):
	    honor_issue(argv[2], iid) 
    elif CMD == 'comment':
	for cid in resolve_cables(argv[3:]):
	    comment_cable(cid, argv[2]) 
    elif CMD == 'sibling':
	source_cid = resolve_cable(argv[3]) 
	if source_cid:
	    for cid in resolve_cables(argv[4:]):
		add_sibling(cid, source_cid['cid'], argv[2]) 
#   else:
	#dump_help() 

release_db()

