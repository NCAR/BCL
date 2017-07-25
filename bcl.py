#!/usr/bin/python
from sys import path, argv, stderr
path.append("/ssg/bin/python_modules/")
import extraview_cli
from nlog import vlog,die_now
from ClusterShell.NodeSet import NodeSet
from ClusterShell.Task import task_self
import sqlite
import os
import syslog
import pbs
import siblings
import cluster_info
import ib_diagnostics
import ib_mgt
import pprint
import time
import datetime
import re
import csv
import sys

def initialize_db():
    """ Initialize DATABASE state variable 
    Attempts to load Yaml file but will default to clean state table
    """
    global BAD_CABLE_DB, SQL_CONNECTION, SQL

    (SQL_CONNECTION, SQL) = sqlite.init(BAD_CABLE_DB)
    if not SQL_CONNECTION or not SQL:
	die_now('unable to open DB')

    SQL.executescript("""
	PRAGMA foreign_keys = ON;

 	create table if not exists cables (
	    cid INTEGER PRIMARY KEY AUTOINCREMENT,
	    state TEXT,
	    --creation time
	    ctime INTEGER,
	    --mtime (last time state from watch state)
	    mtime INTEGER,
	    --Cable Length
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

    """)

    #sqlite.add_column(SQL, 'cable_ports' , 'enabled', 'BOOLEAN')

def release_db():
    """ Releases Database """
    global BAD_CABLE_DB, SQL_CONNECTION, SQL
    sqlite.close(SQL_CONNECTION, SQL)
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
		    None,
		    '%s: Bad Cable %s' % (cluster_info.get_cluster_name_formal(), cname), 
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
		EV.assign_group(tid, 'ssg', None, {
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
		    sibling = NULL,
		    mtime = ?
		WHERE
		    cid = ?
		;''', (
		    suspected,
		    tid,
		    timestamp,
		    cid
	    ));

	    vlog(3, 'Changed cable %s to suspect state %s times' % (cid, suspected))

	#update EV if cable is not disabled. disabled cables will get issues that will confuse ops
	if row['state'] != 'disabled' and not DISABLE_TICKETS:
	    EV.add_resolver_comment(tid, 'Bad Cable Issue:\nType: %s\nIssue: %s\nSource: %s\n%s' % (
		issue_type, 
		issue, 
		source,
		raw
	    ))


def resolve_cable(needle):
    """ Resolve user inputed string for cable (needle)

    Honored formats:
	Cable: c#
	ticket: t#
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
		t(?P<ticket>[0-9]+)
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

    if match.group('cid') or match.group('label') or match.group('label') or match.group('ticket'):
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
		cables.flabel = ? or
		cables.ticket = ?

	    ORDER BY cables.ctime DESC
	    LIMIT 1
	''',(    
	    match.group('cid'),
	    match.group('label'),
	    match.group('label'),
	    match.group('ticket')
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

    if not user_input:
	return [ None ]

    state_match = re.compile(
	r"""
	    ^\s*@(?:bad:|)(?P<state>\w+)\s*$
	""",
	re.VERBOSE
	) 

    for needle in user_input:
	match = state_match.match(needle)
	if match:
	    SQL.execute('''
		SELECT 
		    cables.cid
		FROM 
		    cables
		WHERE
		    cables.state = ?
	    ''',(
		match.group('state'),
	    ))

	    for row in SQL.fetchall():
		if row['cid'] and not row['cid'] in cids:
		    cids.append(row['cid'])
	else:
	    cret = resolve_cable(needle)
	    vlog(4, 'resolving %s to %s' %(needle, cret))

	    if cret:
		if not cret['cid'] in cids:
		    cids.append(cret['cid'])
	    else:
		vlog(2, 'unable to resolve %s to a known cable or port' % (needle))

    return cids

def resolve_cable_ports(user_input):
    """ Resolve user inputed set of strings into cable port id list """

    cpids = []

    if not user_input:
	return [ None ]

    for needle in user_input:
	cret = resolve_cable(needle)
	vlog(4, 'resolving %s to %s' %(needle, cret))

	if cret:
	    if not cret['cpid'] in cpids:
		cpids.append(cret['cpid'])
	else:
	    vlog(2, 'unable to resolve %s to a known port' % (needle))

    return cpids

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

    SQL.execute('''
	UPDATE
	    issues 
	SET
	    ignore = 1
	WHERE
	    iid = ?
	;''', (iid,));

    vlog(2, 'issue i%s will be ignored: %s' % (iid, comment))

def honor_issue(comment, iid):
    """ Honor issue """

    SQL.execute('''
	UPDATE
	    issues 
	SET
	    ignore = 0
	WHERE
	    iid = ?
	;''', (iid,));

    vlog(2, 'issue i%s will be honored: %s' % (iid, comment))

def set_plabel_cable(cid, plabel):
    """ set physical label of cable  """

    if plabel == '':
	plabel = None

    SQL.execute('''
	SELECT 
	    cid,
	    plabel
	FROM 
	    cables
	WHERE
	    cables.cid = ?
	LIMIT 1
    ''',(
	cid,
    ))

    for row in SQL.fetchall():
	vlog(2, 'set plabel from %s to %s for cable c%s' % (row['plabel'], plabel, cid))

	SQL.execute('''
	    UPDATE
		cables 
	    SET
		plabel = ?
	    WHERE
		cid = ?
	    ;''', (
		plabel,
		cid
	));

def set_plabel_cableport(cpid, plabel):
    """ set physical label of cable port """

    if plabel == '':
	plabel = None

    SQL.execute('''
	SELECT 
	    cpid,
	    plabel
	FROM 
	    cable_ports
	WHERE
	    cpid = ?
	LIMIT 1
    ''',(
	cpid,
    ))

    for row in SQL.fetchall():
	vlog(2, 'set plabel from %s to %s for cable port p%s' % (row['plabel'], plabel, cpid))

	SQL.execute('''
	    UPDATE
		cable_ports 
	    SET
		plabel = ?
	    WHERE
		cpid = ?
	    ;''', (
		plabel,
		cpid
	));
 

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


def enable_cable_ports(cid):
    """ Enables cable ports in fabric """

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
	    vlog(3, 'skip enabling hca for p%s' % ( row['cpid'] ))
	else:
	    ib_mgt.enable_port(int(row['guid']), int(row['port'])) 

def remove_cable(cid, comment):
    """ marks cable as removed """
    
    release_cable(cid, comment)
    
    SQL.execute('''
        UPDATE
            cables 
        SET
            state = 'removed',
            comment = ?,
            sibling = NULL
        WHERE
            cid = ?
        ;''', (
            comment,
            cid
    ));
    
    vlog(2, 'Marked c%s as removed: %s' % (cid, comment))
 
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

def query_cable_ports(cid):
    """ Queries cable ports in fabric """

    SQL.execute('''
	SELECT 
	    guid,
	    port,
	    flabel
	FROM 
	    cable_ports 
	WHERE
	    cid = ?
    ''',(
	cid,
    ))

    for row in SQL.fetchall(): 
	ret = ib_mgt.query_port(int(row['guid']), int(row['port']))
	for sm,txt in ret.iteritems():
	    for field in txt:
		for line in field.split('\n'):
		    print '%s: %s' % (row['flabel'], line)
 

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
	    row['length'] if row['length'] else 'Unknown',
	    row['SN'] if row['SN'] else 'Unknown',
	    row['PN'] if row['PN'] else 'Unknown',
	    row['ticket']
	))

    #EV.assign_group(tid, 'casg', None, {
    if not DISABLE_TICKETS:
	vlog(3, 'Sent Ticket %s to CASG' % (tid))

	#provide physical label if one is known
	if plabel:
	    label = 'Physical Cable Label: %s\nSoftware Cable Label: %s' % (plabel, flabel)
	else:
 	    label = 'Cable Label: %s' % (flabel)

	EV.assign_group(tid, 'casg', None, {
	    'COMMENTS':	'''
		CASG,

		The follow cable has been marked for repairs and has been disabled.
		This cable has had %s events that required repair to date.

		%s
		Length: %s
		Serial: %s
		Product Number: %s

		SSG Request:

		%s

		The following cables have also been shutdown for this repair work:
		%s

		Please verify that the cable ports are dark before repairing cable or return ticket noting the cables are not disabled.
		If there are any questions or issues, please return this ticket to SSG with details.
	    ''' % (
		    suspected,
		    label,
		    length if length else 'Unknown',
		    SN if SN else 'Unknown',
		    PN if PN else 'Unknown',
		    comment,
		    "\n\n".join(siblings) if siblings  else 'No siblings cables at this time.'
	    )
	});
     
def dump_inventory():
    """ Dumps inventory """

    SQL.execute('''
	SELECT 
	    SN,
	    PN,
	    length,
	    cp1.flabel as cp1_flabel,
	    cp2.flabel as cp2_flabel
	FROM 
	    cables

	INNER JOIN
	    cable_ports as cp1
	ON
	    cables.cid = cp1.cid

	INNER JOIN
	    cable_ports as cp2
	ON
	    cables.cid = cp2.cid and
	    cp1.cpid != cp2.cpid

 	WHERE
	    state != 'removed' and
	    cp1.cpid IS NOT NULL and
	    cp2.cpid IS NOT NULL and
	    SN IS NOT NULL and
	    PN IS NOT NULL and
	    SN != ""
 
    ''')

    csvw = csv.writer(sys.stdout, delimiter=',', quotechar='"', quoting=csv.QUOTE_NONNUMERIC)
    csvw.writerow(['serial_number', 'product_number', 'length', 'firmware_label_port_1', 'firmware_label_port_2'])
    for row in SQL.fetchall():
	csvw.writerow(row)
 
def enable_cable(cid, comment):
    """ enable cable """

    SQL.execute('''
	SELECT 
	    cables.cid,
	    cables.flabel,
	    cables.state,
	    cables.ticket
	FROM 
	    cables
        WHERE
	    cables.cid = ?
	LIMIT 1
    ''',(
	cid,
    ))

    for row in SQL.fetchall():
	vlog(3, 'enabling cable c%s: %s' % (cid, comment))
	enable_cable_ports(cid)

	if row['state'] == 'disabled':
	    vlog(3, 'Disabled cable c%s returned to suspect state' % (cid))
	    SQL.execute('''
		UPDATE
		    cables 
		SET
		    state = 'suspect',
		    comment = ?
		WHERE
		    cid = ?
		;''', (
		    comment,
		    cid
	    ));

 	if row['ticket'] and not DISABLE_TICKETS:
	    EV.add_resolver_comment(row['ticket'], 'Cable %s enabled.' % (row['flabel']))
	    vlog(3, 'Update Extraview Ticket %s for c%s was enabled' % (row['ticket'], cid))
 
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
	if row['state'] == 'sibling':
	    vlog(1, 'disabling sibling cable c%s.' % (cid))
	elif row['state'] == 'disabled':
 	    vlog(1, 'cable already disabled. ignoring request to disable c%s again.' % (cid))
	    return                    
 	elif row['state'] == 'watch':
 	    vlog(3, 'cable in watch state. adding c%s to bad cable list.' % (cid))
	    add_issue('Manual Entry', cid, comment, 'Disable cable requested. Auto adding cable to bad cables.', 'admin', int(time.time()))

	vlog(3, 'disabling cable c%s.' % (cid))

 	if row['state'] != 'sibling': 
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
	    enable_cable(cid, comment) 
	    vlog(3, 'enabling and ignoring release cable c%s from state %s' % (cid, row['state']))
	    return

	vlog(3, 'release cable c%s from state %s' % (cid, row['state']))

        enable_cable_ports(cid)

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

    if what == 'action' or what == 'actions' or what == 'actionable':
	f='{0:<10}{1:<10}{2:<15}{3:<12}{4:<15}{5:<15}{6:<70}'
 	print f.format(
		"cable_id",
		"state",
		"Ticket",
		"length",
		"Serial_Number",
		"Product_Number",
		"Firmware Label (node_desc)"
	    )             

	for cid in resolve_cables(list_filter):
	    SQL.execute('''
		SELECT 
		    cid,
		    length,
		    SN,
		    PN,
		    state,
		    comment,
		    suspected,
		    ticket,
		    flabel,
		    mtime
		FROM 
		    cables
		WHERE
		    ( 
			? IS NULL and
			state != 'watch' and
			state != 'removed' and
			sibling IS NULL
		    ) or cid = ? 
		ORDER BY 
		    ctime 
	    ''', (
		cid,
		cid
	    ))

	    for row in SQL.fetchall():
		print f.format(
			'c%s' % (row['cid']),
			row['state'],
			't%s' % (row['ticket']) if row['ticket'] else None,
			row['length'] if row['length'] else None,
			row['SN'] if row['SN'] else None,
			row['PN'] if row['PN'] else None,
			row['flabel']
		    ) 
		print '\tSuspected %s times. Last went suspect on %s' % (
			row['suspected'], 
			datetime.datetime.fromtimestamp(row['mtime']).strftime('%Y-%m-%d %H:%M:%S') if row['mtime'] > 0 else None
		    )
		print '\tComment: %s' % (row['comment'])

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
		    FROM 
			issues 
		    WHERE
			ignore = 0 and 
			cid = ? and
			mtime >= ?
		    ORDER BY iid ASC
		''', (
		    int(row['cid']),
		    int(row['mtime']) if row['mtime'] else None,
		))

		for irow in SQL.fetchall():
		    print '\tIssue %s %s: %s' % (
			    'i%s' % irow['iid'],
			    irow['source'],
			    irow['issue']
			) 

		print ' '

    elif what == 'cables' or what == 'cable':
        f='{0:<10}{1:<10}{2:10}{3:<12}{4:<15}{5:<15}{6:<15}{7:<15}{8:<15}{9:<15}{10:<50}{11:<50}{12:<50}'
 	print f.format(
		"cable_id",
		"sibling",
		"state",
		"Suspected#",
		"Ticket",
		"ctime",
		"mtime",
		"length",
		"Serial_Number",
		"Product_Number",
		"Comment",
		"Firmware Label (node_desc)",
		"Physical Label"
	    )            
	for cid in resolve_cables(list_filter):
	    SQL.execute('''
		SELECT 
		    cables.cid as cid,
		    cables.sibling as scid,
		    cables.ctime as ctime,
		    cables.mtime as mtime,
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
		    ( ? IS NULL or cables.cid = ? )

		LEFT OUTER JOIN
		    cable_ports as cp2
		ON
		    cables.cid = cp2.cid and
		    cp2.cpid != cp1.cpid

		GROUP BY cables.cid
	    ''', (
		cid, cid
	    ))

	    for row in SQL.fetchall():
		print f.format(
			'c%s' % (row['cid']),
			'c%s' % (row['scid']) if row['scid'] else None,
			row['state'],
			row['suspected'],
			't%s' % (row['ticket']) if row['ticket'] else None,
			row['ctime'],
			row['mtime'],
			row['length'] if row['length'] else None,
			row['SN'] if row['SN'] else None,
			row['PN'] if row['PN'] else None,
			row['comment'],
			row['flabel'],
			row['plabel']
		    )

    elif what == 'ports' or what == 'port':
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

	for cid in resolve_cables(list_filter):
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
		    cid = ?
		ORDER BY cpid ASC
	    ''', (
		cid, cid
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
        f='{0:<10}{1:<15}{2:<10}{3:<10}{4:<15}{5:<20}{6:<100}{7:<50}'
 	print f.format(
		"issue_id",
		"Type",
		"cable_id",
		"Ignored",
		"mtime",
		"source",
		"issue",
		"raw error"
	    ) 

	for cid in resolve_cables(list_filter):
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
		FROM 
		    issues 
		WHERE
		    ? IS NULL OR
		    cid = ?
		ORDER BY iid ASC
	    ''', (cid, cid))

	    for row in SQL.fetchall():
		print f.format(
			'i%s' % row['iid'],
			row['type'],
			'c%s' % row['cid'] if row['cid'] else None,
			'False' if row['ignore'] == 0 else 'True',
			row['mtime'],
			row['source'],
			row['issue'],
			row['raw'].replace("\n", "\\n") if row['raw'] else None
		    )

    else:
	vlog(1, 'unknown list %s request' % (list_filter))

def convert_guid_intstr(guid):
    """ normalise representation of guid to string of an integer 
	since sqlite cant handle 64bit ints """
    return str(int(guid, 16))

def run_parse(dump_dir):
    """ Run parse mode against a dump directory """
    global EV, SQL

    def gv(port, key):
	""" get value or none """
	if not port:
	    return None
	else:
	    return None if not key in port else str(port[key])
    
    def find_cable(port1, port2):
	""" Find (and update) cable in db 
	port1: ib_diagnostics formatted port
	port2: ib_diagnostics formatted port
	"""

	vlog(5, 'find_cable %s/P%s %s/P%s' % (gv(port1,'guid'), gv(port1,'port'), gv(port2,'guid'), gv(port2,'port')))
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
		cables.cid as cid,
		max(cp1.hca, cp2.hca) as hca
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
	    return {'cid': rows[0]['cid'], 'hca': rows[0]['hca']}
	else:
	    return None

    def insert_cable(port1, port2, timestamp):
	""" insert new cable into db """

	plabel = None
	port_plabel = { 'port1': None, 'port2': None }

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

    def check_replaced_cable(cid, SN, PN):
	""" check if new cable was a cable replacement """

	#find if this cable already existed but with a different field (SN or PN)
	SQL.execute('''
	    SELECT 
		cid,
		SN,
		PN,
		ctime,
		state

	    FROM 
		cables

	    WHERE
		cid != ? and
		SN = ? and
		PN = ? and
		state != 'removed'

	    ORDER BY ctime DESC
	    LIMIT 1
	''',(
	    cid,
	    SN,
	    PN
	))

	for row in SQL.fetchall():
	    what = 'detected cable SN/PN change c%s and c%s from %s/%s to %s/%s' % (
		cid, 
		row['cid'],
		row['SN'],
		row['PN'],
		SN, 
		PN
	    )

	    if row['state'] == 'disabled' or row['state'] == 'suspect':
		#cable was probably replaced on purpose
		#mark old cable as removed
		remove_cable(row['cid'], what)
	    elif row['state'] == 'sibling':
		#ignore new SN since it will likely be reversed
		comment_cable(cid, what)  
 
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
	ib_diagnostics.parse_ibdiagnet_csv(ports, issues, fds)

    with open('%s/%s' % (dump_dir,'ibdiagnet2.log') , 'r') as fds:
	ib_diagnostics.parse_ibdiagnet(ports, issues, fds.read()) 

    if os.path.isfile('%s/%s' % (dump_dir,'ibdiagnet2.cables')):
	with open('%s/%s' % (dump_dir,'ibdiagnet2.cables'), 'r') as fds:
	    ib_diagnostics.parse_ibdiagnet_cables(ports, issues, fds.read()) 

    p_ibcv2 = '%s/%s' % (dump_dir,'sgi-ibcv2.log') #optional
    if os.path.isfile(p_ibcv2):
	with open(p_ibcv2, 'r') as fds:
	    ib_diagnostics.parse_sgi_ibcv2(ports, issues, fds.read()) 
	    pass

    ibsp = cluster_info.get_ib_speed()
    ib_diagnostics.find_underperforming_cables ( ports, issues, ibsp['speed'], ibsp['width'])

    #add every known cable to database
    #slow but keeps sane list of all cables forever for issue tracking
    known_cables=[] #track every that is found
    new_cables=[] #track every that is created
    hca_cables=[] #track every that has an hca
    for port in ports:
	port1 = port
	port2 = port['connection']
	cid = None
	hca_found = None
	retc = find_cable(port1, port2)

	if not retc:
	    #create the cable
	    cid = insert_cable(port1, port2, timestamp)
	    new_cables.append({ 'cid': cid, 'port1': port1, 'port2': port2})
	    hca_found = port1['type'] == "CA" or (port2 and port2['type'] == "CA")
	else:
	    cid = retc['cid']
	    hca_found = retc['hca']

	#record cid in each port to avoid relookup
	port1['cable_id'] = cid
	if port2:
	    port2['cable_id'] = cid

	known_cables.append(cid)
	if hca_found:
	    hca_cables.append(cid)

    #check new cables (outside of begin/commit)
    for cable in new_cables:
	cid = cable['cid']
	port1 = cable['port1']
	port2 = cable['port2']

	#find if this cable already existed but with a different field (SN or PN)
	if gv(port, 'SN') and gv(port, 'PN'):
	    check_replaced_cable(cid, gv(port, 'SN'), gv(port, 'PN'))

    #Find any cables that are known but not parsed this time around (aka went dark)
    #ignore any cables in a disabled state
    missing_cables = []
    #find all cables marked as sibling
    sibling_cables = {} #sibling cable -> bad cable
    SQL.execute('''
	    SELECT 
		cid,
		state,
		sibling
	    FROM 
		cables
	    WHERE
		state != 'removed' and
		state != 'disabled'
	''')
    for row in SQL.fetchall():
	if row['state'] == 'sibling':
	    sibling_cables[int(row['cid'])] = int(row['sibling'])
	elif not row['cid'] in known_cables:
	    missing_cables.append(int(row['cid']))

    for cid in missing_cables:
	#Verify missing cables actually matter: ignore single port cables (aka unconnected)
	#ignore missing cables connected to HCAs, nodes go up and down all the time
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
		cables.cid = cp1.cid and
		cp1.hca != 0

	    LEFT OUTER JOIN
		cable_ports as cp2
	    ON
		cables.cid = cp2.cid and
		cp1.cpid != cp2.cpid and
		cp2.hca != 0
		 
	    LIMIT 1 
	    ''', (
		cid,
	    ))
	for row in SQL.fetchall():
	    if row['cp1_cpid'] and row['cp2_cpid']:
		#cable went dark
		add_issue(
		    'missing',
		    cid,
		    'Cable went missing',
		    None,
		    'ibnetdiscover -p',
		    timestamp
		)              
	    else:
		vlog(3, 'Ignoring missing single port Cable %s' % (cid))

    ticket_issues = []
    for issue in issues:
	cid = None

	#set cable from port which was just resolved
	for iport in issue['ports']:
	    if iport and 'cable_id' in iport:
		cid = iport['cable_id']
 
	#put blame from sibling on the marked bad cable
	if cid in sibling_cables:
	    vlog(3, 'blaming c%s instead of %s' % (cid, sibling_cables[cid]))
	    cid = sibling_cables[cid]

 	if issue['type'] == 'missing' and cid in hca_cables:
	    vlog(3, 'ignoring missing cable c%s with an hca' % (cid))
	    continue
             
	vlog(5, 'issue detected: %s' % ([
	    issue['type'],
	    'c%s' % cid if cid else None,
	    issue['issue'],
	    issue['raw'],
	    issue['source'],
	    timestamp
	]))

	if cid:
	    #hand over cleaned up info for issues
	    add_issue(
		issue['type'],
		cid,
		issue['issue'],
		issue['raw'],
		issue['source'],
		timestamp
	    )
	else:
	    #issues without a known cable will be aggregated into a single ticket
	    ticket_issues.append(issue['raw'])

    SQL.execute('VACUUM;')

    #create ticket if are non cable issues
    if ticket_issues and not DISABLE_TICKETS:
	tid = EV.create( 
	    'ssgev',
	    'ssg',
	    None,
	    '%s: Infiniband Issues' % (cluster_info.get_cluster_name_formal()), 
	    '''
	    %s issues have been detected against the Infinband fabric for %s.

	    Raw Data: %s
	    ''' % (
		len(ticket_issues),
		cluster_info.get_cluster_name_formal(),
		dump_dir
	    ), { 
		'HELP_LOCATION': EV.get_field_value_to_field_key('HELP_LOCATION', 'NWSC'),
		'HELP_HOSTNAME': EV.get_field_value_to_field_key(
		    'HELP_HOSTNAME', 
		    cluster_info.get_cluster_name_formal()
		),
		'HELP_HOSTNAME_OTHER': 'Infiniband Fabric'
	}) 

	vlog(3, 'Created Ticket %s against fabric issues' % (tid))

	#combine the issues and try to not crash EV
	i = 0
	buf = ''
	for msg in ticket_issues:
	    buf += msg + "\n"
	    i += 1

	    if i == 200: #magic guessed number that EV can take
		EV.add_resolver_comment(tid, buf)
		buf = ''
		i = 0
	if buf != '':
	    EV.add_resolver_comment(tid, buf)


def dump_help(full = False):
    if full == True:
	print("""NCAR Bad Cable List Multitool (Full Help)

	help: {0}
	    Print this help message

	list: 
	    {0} list
	    {0} list action[s] {{cables}}+ 
	    {0} list actionable {{cables}}+ 
		dump list of actionable cable issues
     
	    {0} list issues {{cables}}+ 
		dump list of issues

	    {0} list cables {{cables}}+
		dump list of cables

	    {0} list ports {{cables}}+ 
		dump list of cable ports

	add: 
	    {0} add {{issue description}} {{cables}}+ 
	    {0} suspect {{issue description}} {{cables}}+ 
		Note: Use GUID/Port syntax or cable id (c#) to avoid applying to wrong cable
		add cable to bad node list 
		open EV against node in SSG queue or Assign to SSG queue

	sibling:
	    {0} sibiling 'comment' {{(bad cable id) c#}} {{cables}}+ 
	    {0} donor 'comment' {{(bad cable id) c#}} {{cables}}+ 
		mark cable as sibling to bad cable if sibling is in watch state
		disables sibling cable in fabric

	disable: {0} disable 'comment' {{cables}}+ 
	    disables cable in fabric
	    add cable to bad cable list (if not one already) unless cable is sibling

	enable: {0} enable 'comment' {{cables}}+ 
	    enables cable in fabric
	    puts a cable in disabled state back into suspect state (use release to set cable state to watch)
     
	casg: {0} casg {{comment}} {{cables}}+ 
	    disables cable in fabric
	    send extraview ticket to CASG

	query: {0} query {{cables}}+ 
	    Query cables status in fabric
     
	release: 
	    {0} release {{comment}} {{cables}}+ 
	    {0} resolve {{comment}} {{cables}}+ 
		enable cable in fabric
		set cable state to watch
		close Extraview ticket
		release any sibling cables
		release sibling status of cable (don't consider cable as sibling anymore)

	rejuvenate: {0} rejuvenate {{comment}} {{cables}}+ 
	    Note: only use this if the cable has been replaced and it was not autodetected
	    release cable
	    sets the suspected count back to 0
	    disassociate Extraview ticket from Cable

	remove: {0} remove {{comment}} {{cables}}+ 
	    Note: only use this if the cable has been removed/replaced permanently
	    release cable
	    sets the cable as removed (disables all future detection against cable)

	cable_plabel: {0} cable_plabel {{physical label}} {{cables}}+ 
	    Assign cable new physical label. (label can be an empty string to use logical label)

 	port_plabel: {0} cable_plabel {{physical label}} {{cable ports}}+ 
	    Assign cable port new physical label. (label can be an empty string to use logical label)
 
	comment: {0} comment {{comment}} {{cables}}+ 
	    add comment to bad cable's extraview ticket 

	ignore: {0} ignore {{comment}} {{(issue id) i#}}+
	    Note: only use this in special cases for issues that can not be fixed
	    ignore issue with assigned cable until honor requested

	honor: {0} honor {{comment}} {{(issue id) i#}}+
	    removes ignore status of issue

 	inventory: {0} inventory 
	    dumps CSV with format: serial,product_number,length,end point label,end point label
     
	parse: {0} parse {{path to ib dumps dir}}
	    reads the output of ibnetdiscover, ibdiagnet2 and ibcv2
	    generates issues against errors found 
	    checks if any cable has been replaced (new SN) and will set that cable back to watch state

	Cable Labels Types: (aka {{cables}}+)
	    cable id: c#
	    ticket id: t#
	    guid/port pairs: S{{guid}}/P{{port}}
	    label: cable port label
	    port id: p#
	    states: @watch, @suspect, @disabled, @sibling, @removed

	Optional Environment Variables:
	    VERBOSE={{1-5 default=3}}
		1: lowest
		5: highest
		
	    DISABLE_TICKETS={{YES|NO default=NO}}
		YES: disable creating and updating tickets (may cause extra errors)
		NO: create tickets

	    BAD_CABLE_DB={{path to sqlite db defailt=/etc/ncar_bad_cable_list.sqlite}}
		Warning: will autocreate if non-existant or empty
		Override which sqlite DB to use.
     
	""".format(argv[0]))
    else: #short error mode
	stderr.write("""NCAR Bad Cable List Multitool (Quick Help)

	list: {0} list 
	    dump list of actionable cable issues

	disable: {0} disable 'comment' {{cables}}+ 
	    disables cable in fabric

	enable: {0} enable 'comment' {{cables}}+ 
	    enables cable in fabric
     
	casg: {0} casg {{comment}} {{cables}}+ 
	    disables cable in fabric
	    send extraview ticket to CASG

	query: {0} query {{cables}}+ 
	    Query cables status in fabric (ibportstate query output)
     
	release: {0} release {{comment}} {{cables}}+ 
		enable cable in fabric and set cable state to watch
		close Extraview ticket

	comment: {0} comment {{comment}} {{cables}}+ 
	    add comment to bad cable's extraview ticket 

	""".format(argv[0]))

if not cluster_info.is_mgr():
    die_now("Only run this on the cluster manager")

BAD_CABLE_DB='/etc/ncar_bad_cable_list.sqlite'
""" const string: Path to JSON database for bad cable list """

DISABLE_TICKETS=False
EV = None

if 'BAD_CABLE_DB' in os.environ and os.environ['BAD_CABLE_DB']:
    BAD_CABLE_DB=os.environ['BAD_CABLE_DB']
    vlog(1, 'Database: %s' % (BAD_CABLE_DB))

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
    elif len(argv) < 3:
	if CMD == 'help':
	    dump_help(True)  
	elif CMD == 'list':
	    list_state('action', None)   
 	elif CMD == 'inventory':
	    dump_inventory()
	else:
	    dump_help()  
    else:
	if CMD == 'list':
	    list_state(argv[2].lower(), argv[3:] if len(argv) > 3 else None)  
	elif CMD == 'remove':
	    for cid in resolve_cables(argv[3:]):
		remove_cable(cid, argv[2]) 
	elif CMD == 'disable':
	    for cid in resolve_cables(argv[3:]):
		disable_cable(cid, argv[2])
	elif CMD == 'enable':
	    for cid in resolve_cables(argv[3:]):
		enable_cable(cid, argv[2]) 
	elif CMD == 'casg':
	    for cid in resolve_cables(argv[3:]):
		send_casg(cid, argv[2]) 
	elif CMD == 'add' or CMD == 'suspect':
	    for cid in resolve_cables(argv[3:]):
		add_issue('Manual Entry', cid, argv[2], None, 'admin', int(time.time()))
	elif CMD == 'release' or CMD == 'resolve':
	    for cid in resolve_cables(argv[3:]):
		release_cable(cid, argv[2])
 	elif CMD == 'query':
	    for cid in resolve_cables(argv[2:]):
		query_cable_ports(cid) 
	elif CMD == 'rejuvenate':
	    for cid in resolve_cables(argv[3:]):
		release_cable(cid, argv[2], True)
	elif CMD == 'ignore':
	    for iid in resolve_issues(argv[3:]):
		ignore_issue(argv[2], iid)
	elif CMD == 'honor':
	    for iid in resolve_issues(argv[3:]):
		honor_issue(argv[2], iid) 
 	elif CMD == 'cable_plabel':
	    for cid in resolve_cables(argv[3:]):
		set_plabel_cable(cid, argv[2])
  	elif CMD == 'port_plabel':
	    for cid in resolve_cable_ports(argv[3:]):
		set_plabel_cableport(cid, argv[2])
	elif CMD == 'comment':
	    for cid in resolve_cables(argv[3:]):
		comment_cable(cid, argv[2]) 
	elif CMD == 'sibling' or CMD == 'donor':
	    source_cid = resolve_cable(argv[3]) 
	    if source_cid:
		for cid in resolve_cables(argv[4:]):
		    add_sibling(cid, source_cid['cid'], argv[2]) 
	else:
	    dump_help() 

release_db()

