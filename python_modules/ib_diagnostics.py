#!/usr/bin/env python
import syslog
import argparse #yum install python-argparse.noarch
import os
import socket
import subprocess 
import pipes
import sys
import re
import shlex
from datetime import datetime
import fcntl
import csv

def msg(*strmsg):
    """ Send message to user """
    global args, reportfile
    out = ' '.join(map(str,strmsg)) + '\n'
    sys.stderr.write(out)
    syslog.syslog(out)
    reportfile.write(out)
def vmsg(*strmsg):
    """ Send verbose message to user """
    global args
    if args.verbose:
	msg(strmsg)
def exec_to_file ( cmd, output_file ):
    """ Runs cmd and pipes STDOUT to output_file """
    fo = open(output_file, 'w')
    if args.verbose:
	msg('Running command: %s' % cmd) 
	msg('Dumping command STDOUT: %s' % output_file) 
	p = subprocess.Popen(cmd, stdout=fo, cwd='/tmp/')
    else:
	p = subprocess.Popen(cmd, stdout=fo, stderr=FNULL, cwd='/tmp/')
    p.wait()
    fo.flush()
    fo.close()
    return p.returncode
def exec_to_string_with_input ( cmd, input):
    """ Runs cmd, sends input to STDIN and places Return Value, STDOUT, STDERR into returned list  """
    if args.verbose:
	msg('Running command: %s' % cmd) 
    try:
	p = subprocess.Popen(cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, cwd='/tmp/')
	stdout, stderr = p.communicate(input=input)
	return [p.returncode, stdout, stderr ]
    except:
	msg('Command %s failed' % cmd)
	return [-1, '', 'Failed to run']
def exec_to_string ( cmd, cwd='/tmp/' ):
    """ Runs cmd and places Return Value, STDOUT, STDERR into returned list  """
    if args.verbose:
	msg('Running command: %s' % cmd) 
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, cwd=cwd)
    stdout, stderr = p.communicate()
    return [p.returncode, stdout, stderr ]
def exec_opensm_to_string ( cmd ):
    """ Runs cmd on openSM host and places Return Value, STDOUT, STDERR into returned list  """
    return exec_to_string (['/usr/bin/ssh','-n','-o','BatchMode=yes', args.sm, cmd] )
def exec_opensm_to_file ( cmd, output_file ):
    """ Runs cmd on openSM host and pipes STDOUT to output_file """
    return exec_to_file (['/usr/bin/ssh','-n','-o','BatchMode=yes', args.sm, cmd], output_file )
def opath ( file_name ):
    """ Takes file_name and returns full path to file in output directory """
    return "%s/%s" % (output_directory, file_name)
def write_file ( file_name, contents ):
    """ Takes file_name and writes contents to file. it will clobber file_name. """
    if args.verbose:
	msg('Writing File: %s SIZE=%s' % (file_name, len(contents)))
    file = open(file_name, 'w')
    file.write(contents)
    file.close()
def parse_port ( label ):
    """ Parse the name of a IB port 
    returns dictionary with parsed values

    Known Formats:
	'ys4618 HCA-1'(4594/1)
	'ys4618 HCA-1'(4594/1)
	MF0;ys75ib1:SXX536/L05/U1/P2
	ys75ib1/L05/U1/P2
	ys46ib1:SX60XX/U1/P26
	MF0;ca00ib1a:SXX512/S01/U1
	'MF0;ys72ib1:SXX536/L22/U1'(395/1)
	geyser1/H3/P1
	ys70ib1 L05 P12
	ys22ib1 P13 
    	ys2324 HCA-1
    	geyser01 HCA-1 P3

    """
    global args

    name  = None
    hca   = None
    leaf  = None
    spine = None #only used for internal orca connections
    port  = None   

    global ib_portname_type1_regex
    global ib_portname_type2_regex
    if ib_portname_type1_regex == None:
	#regex matches following:
	#'ys4618 HCA-1'(4594/1)
	#'ys4618 HCA-1'(4594/1)
	#MF0;ys75ib1:SXX536/L05/U1/P2
	#ys75ib1/L05/U1/P2
	#ys46ib1:SX60XX/U1/P26
	#MF0;ca00ib1a:SXX512/S01/U1
	#'MF0;ys72ib1:SXX536/L22/U1'(395/1)
	#geyser1/H3/P1
	ib_portname_type1_regex = re.compile(
		r"""
		^\s*
		(?:\'|)
		(?:
		    (?P<hca_host_name>\w+)\s+			#Host name
		    [hcaHCA]+-(?P<hca_id>\d+)			#HCA number
		    |                          	
		    (?:MF0;|)					#MF0 - useless id	
		    (?P<tca_host_name>\w+)			#TCA Name
		    (?::SX\w+|)					#Switch Type
		    (?:\/[hcaHCA]{1,3}(?P<hca_id2>\d+)|)	#HCA number
		    (?:\/[lLiIdD]+(?P<leaf>\d+)|)		#Leaf (sometimes called /LID in error)
		    (?:\/S(?P<spine>\d+)|)			#Spine
		    (?:\/U\d+|)					#U number
		    (?:\/P(?P<port1>\d+)|)			#Port
		)
		(?:
		    (?:\'|)
		    \(
			\d+					#LID: just assume it is wrong
			\/
			(?P<port2>\d+)				#Port
		    \)
		    |
		)
		\s*$
		""",
		re.VERBOSE
		) 

    match = ib_portname_type1_regex.match(label)
    if match:
	#if args.verbose:
	    #msg('matched: %s' % match.group())
	if match.group('hca_host_name'):
	    name = match.group('hca_host_name')
	    hca = match.group('hca_id')
	if match.group('tca_host_name'):
	    name = match.group('tca_host_name')
	    spine = match.group('spine')
	    hca = match.group('hca_id2')
	    leaf = match.group('leaf') 
	if match.group('port1'):
	    port = match.group('port1')
	if match.group('port2'):
	    port = match.group('port2')
    else:
	if ib_portname_type2_regex == None:
	    #regex matches following: (these are usually from human entry)
	    #ys70ib1 L05 P12
	    #ys22ib1 P13 
	    #ys2324 HCA-1
	    #geyser01 HCA-1 P3
	    ib_portname_type2_regex = re.compile(
		    r"""
		    ^\s*
		    (?P<name>\w+)			#name
		    (?:
			(?:\s+
			[hcaHCA]+(?:-|)(?P<hca>\d+)	#hca id
			)
			|
		    )
		    (?:\s+
			[lLiIdD]+			
			(?P<leaf>\d+)			#leaf (called lid in error)
			|
		    )	
		    (?:\s+U\d+|)			#/U useless
		    (?:
			(?:\s+[pP](?P<port>\d+))	#port number
			|
			)
		    \s*$
		    """,
		    re.VERBOSE
		    ) 
	match = ib_portname_type2_regex.match(label)
	if match:
	    if args.verbose:
		msg('matched: %s' % match.group())
	    name = match.group('name')
	    hca = match.group('hca')
	    leaf = match.group('leaf')
	    port = match.group('port')
	else:
	    name = label

    return {
		'name'	: name,
		'hca'	: hca,
		'leaf'	: leaf,
		'spine'	: spine,
		'port'	: port,
		'connection' : None,
		'dumped' : False,
		'serial' : None,
		'length' : None,
		'partnumber' : None,
		'guid' : None,
		'type' : None,
		'speed' : None,
		'width' : None,
		'lid' : None
	    }

def register_cable ( port1, port2 ):
    """ add cable ports to ports list (for now). port2 can be None for unconnected ports. """

    #check for and ignore dups
    for port in ports:
	if  (
		port['lid']    == port1['lid'] and
		port['port']    == port1['port']
	    ) or ( port2 and (
		port['lid']    == port2['lid'] and
		port['port']    == port2['port']
	    )):
		return

    ports.append(port1)
    if port2:
	ports.append(port2)

def parse_ibnetdiscover_cables ( contents ):
    """ Parse the output of 'ibnetdiscover -p' 

    Two types of line formats:
    CA    44  1 0x0002c9030045f121 4x FDR - SW     2 17 0x0002c903006e1430 ( 'localhost HCA-1' - 'MF0;js01ib2:SX60XX/U1' )
    SW     2 19 0x0002c903006e1430 4x SDR                                    'MF0;js01ib2:SX60XX/U1'
    SW    82 19 0x7cfe900300bdf4f0 4x ???                                    'r1i0s0 SW1'

    """
    global args

    ibcable_regex = re.compile(
	    r"""
	    ^(?P<HCA1_type>CA|SW)\s+		#HCA1 type
	    (?P<HCA1_lid>\d+)\s+		#HCA1 LID
	    (?P<HCA1_port>\d+)\s+		#HCA1 Port
	    (?P<HCA1_guid>0x\w+)\s+		#HCA1 GUID
	    (?P<width>\w+)\s+			#Cable Width
	    (?P<speed>\w+|\?\?\?)\s+	        #Cable Speed
	    (
		\'(?P<HCA_name>.+)\'		#Port Name
		|				#cable is connected
		-\s+		
		(?P<HCA2_type>CA|SW)\s+		#HCA2 Type
		(?P<HCA2_lid>\d+)\s+		#HCA2 LID
		(?P<HCA2_port>\d+)\s+		#HCA2 Port
		(?P<HCA2_guid>0x\w+)\s+		#HCA2 GUID
		\(\s+
		    \'(?P<HCA1_name>.+)\'	#HCA1 Name
		    \s+-\s
		    +\'(?P<HCA2_name>.+)\'	#HCA2 Name
		\s+\)
	    )$
	    """,
	    re.VERBOSE
	    ) 
    for line in contents.split(os.linesep):
	match = ibcable_regex.match(line)
	if match:
	    if match.group('HCA_name'):
		port = parse_port(match.group('HCA_name'))
		port['port'] = match.group('HCA1_port')
		port['lid'] = match.group('HCA1_lid')
		port['guid'] = match.group('HCA1_guid')
		port['type'] = match.group('HCA1_type')
		port['speed'] = match.group('speed')
		port['width'] = match.group('width')
		port['connection'] = None
		register_cable(port, None)
		#if args.verbose:
		    #msg(port)
	    else:
		port1 = parse_port(match.group('HCA1_name'))
		port1['port'] = match.group('HCA1_port')
		port1['lid'] = match.group('HCA1_lid')
		port1['type'] = match.group('HCA1_type')
		port1['guid'] = match.group('HCA1_guid')
		port1['speed'] = match.group('speed')
		port1['width'] = match.group('width')

		port2 = parse_port(match.group('HCA2_name'))
		port2['port'] = match.group('HCA2_port')
		port2['lid'] = match.group('HCA2_lid')
		port2['guid'] = match.group('HCA2_guid')
		port2['type'] = match.group('HCA2_type')
		port2['speed'] = match.group('speed')
		port2['width'] = match.group('width')

		#if args.verbose:
		    #msg(port1)
		    #msg(port2)

		#cross reference connecting port
		port1['connection'] = port2
		port2['connection'] = port1
		register_cable(port1, port2)
	else:
	    if args.verbose and line != "":
		msg('Parse fail: %s' % line )
def msg_port_pretty_long ( port, why ): 
    """ msg port label with helpful info"""
    msg('%s: %s SPEED=%s LID=%s GUID=%s SN=%s PN=%s' % (
	    why,
	    port_pretty(port),
	    port['speed'],
	    port['lid'],
	    port['guid'],
	    port['SN'] if 'SN' in port else '',
	    port['PN'] if 'PN' in port else '',
	)
    )
def port_pretty ( port ):
    """ return pretty port label """
    if not port:
	return 'None'
    if port['spine']: #spine
	return '%s/S%s/P%s' % (port['name'], port['spine'], port['port'])
    if port['leaf']: #port on orca
	return '%s/L%s/P%s' % (port['name'], port['leaf'], port['port']) 
    if port['hca']: #hca on node
	return '%s/HCA%s/P%s' % (port['name'], port['hca'], port['port'])
    return '%s/P%s' % (port['name'], port['port']) #tor port

def find_underperforming_cables ( ):
    """ Checks all of the ports for any that are not at full width or speed or disabled """
    global args, ports

    #PhysLinkState:...................LinkUp
    #PhysLinkState:...................Disabled
    #PhysLinkState:...................Polling
    portstate_regex = re.compile(
	    r"""
	    ^(?P<property>\w+):\.+		#property
	    (?P<value>\w+)$			#value
	    """,
	    re.VERBOSE
	    );

    for port in ports:
	if port['name'] == "localhost": #complain about localhost named ports but no need to complain
	    msg('Localhost labeled port: %s <-> %s' % (port_pretty(port), 'N/A' if not port['connection'] else port_pretty(port['connection'])))

	if port['connection']: #ignore unconnected ports
	    if port['speed'] != args.ibspeed:
		msg_port_pretty_long(port, 'Underperforming Port')
	    if port['width'] != '4x': #might want to un-hardcode this
		msg_port_pretty_long(port, 'Underperforming Port')
	else: #check if unconnected ports are disabled
		(ret, out, err) = exec_opensm_to_string('/usr/sbin/ibportstate %s %s query' 
		    % (
			port['lid'],
			port['port']
		    )
		);

		if ret == 0: #this isnt helpful if ibportstate is broke
		    for line in out.split(os.linesep):
			match = portstate_regex.match(line)
			if match:
			    vmsg('Port %s/%s: %s=%s' % (port['lid'], port['port'], match.group('property'), match.group('value')));

			    if match.group('property') == 'PhysLinkState' and match.group('value') == 'Disabled':
				msg_port_pretty_long(port, 'Disabled Port')

def parse_ibdiagnet ( contents ):
    """ Parse the output of 'ib-find-disabled-ports' 
    """
    global args, ports

    ibdiag_line_regex = re.compile(r"""
	    \s*-[^IW]-\s+	    #find all none Info and Warns
	    (?:
		(?!lid=0x[0-9a-z]+\ dev=\d+)		#ignore the lid dumps for counters since its dup
		(?P<msg>.*)				#extract message after type
	    )
	""", re.VERBOSE) 

    for match in re.finditer(r"""
	(?![#-]+[\n\r])[\r\n]*		#all of the stanzas start with --- or ###
	(?P<label>(?![#-]+).*)[\r\n]+   #first real line is the label
	(?P<content>			#content follows label
	    (?:
		(?![#-]+[\n\r])		#make sure not to run into next stanza
		.*[\r\n]*		#suck in the ws
	    )+
	)
	""", contents, re.VERBOSE):

       #Look for summary to extract to ignore it
       if match.group('label') == "Summary":
	   for line in match.group('content').split("\n"):
	       if line == "": #stop at end of the summary count stanza
		   break
       else: #stanza containing detail information
	   for line in match.group('content').split("\n"):
	       lmatch = ibdiag_line_regex.match(line)
	       if lmatch:
		   msg('IBDiagnet2: %s: %s' % (match.group('label'), lmatch.group('msg')))

def parse_ibdiagnet_csv ( path_to_csv ):
    """ Parse the output of ibdiagnet ibdiagnet2.db_csv
    """
    global args, ports

    csv_mode=None
    csv_headers=None

    #START_CABLE_INFO
    #END_CABLE_INFO

    with open(path_to_csv) as fcsv:
	csv_reader = csv.reader(fcsv)
	for row in csv_reader:
	    if len(row) == 1 and row[0] != "" :
		if row[0].startswith('START_'):
		    csv_mode = row[0];
		    csv_headers = None
		if row[0].startswith('END_'):
		    csv_mode = None
		    csv_headers = None
	    else:
		if csv_mode: #in a data block
		    if csv_headers == None:
			csv_headers = row;
		    else: #data
			rowdict = dict(zip(csv_headers, row))

			if csv_mode == 'START_CABLE_INFO':
			    for port in ports:
				 if port['guid'] == rowdict['PortGuid'] and port['port'] == rowdict['PortNum']:
				     #just combine the data into the port
				     port.update(rowdict);
                          
def find_cable_by_switch_leaf_port ( name, leaf, port ):
    """ Checks all of the ports for any that are not at full width or speed """
    global args, ports

    for port in ports:
	if port['name'] == name and port['leaf'] == leaf and port['port'] == port:
	    return port

    return None

syslog.openlog('ibhealthreport')

#disable automation check
if os.path.isfile('/etc/nolocal'):
    print "Error: /etc/nolocal exists. Bailing!"
    exit(1)

parser = argparse.ArgumentParser(description='Dumps IB state and does analysis.')
parser.add_argument('-p','--path', dest='path', help='Log directory Path', required=True)
parser.add_argument('-s','--opensm-host', dest='sm', help='Hostname of opensm server', required=True)
parser.add_argument('-v','--verbose',   dest='verbose', help='Be verbose', action="store_true")
parser.add_argument('-i','--infiniband-speed',   dest='ibspeed', help='Infiniband speed ', choices=['FDR10', 'FDR', 'EDR'], required=True)
parser.add_argument('--sgi-cv2-config',   dest='sgi_cv2_config', help='SGI ibcv2 config file for cluster', required=True)
parser.add_argument('--ib-find-disabled',   dest='ibdisabled', help='Path to OFED MSTK ib-find-disabled-ports.sh script', required=False)
parser.add_argument('--ibdiagnet-time',   dest='ibdiag_time', help='option to pass to -pm_pause_time of ibdiagnet', required=False, default=1200)
args = parser.parse_args()

#only run with lock
LOCK = open('/var/run/ib_diag.py', 'a')
fcntl.flock(LOCK.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)

#output directory name
output_directory=args.path
#open /dev/null
FNULL = open(os.devnull, 'w')
#Create regex that will match all BASH color codes
color_regex = re.compile("\033\[[0-9;]+m")
#globals for later
ib_portname_type1_regex = None 
ib_portname_type2_regex = None
ports = []  #list containing all known ports

#create output directory
try:
    os.mkdir(output_directory, 0755)
except OSError as err:
#ignore pre-existing directories but complain to user
    print err
    pass

#timestamp the directory
timestampfile = open(opath('timestamp.txt'), 'w')
timestampfile.write(datetime.now().strftime('%m/%d/%y %H:%M:%S\n%s\n'))
timestampfile.close();

#dump all output into report file
reportfile = open(opath('report.txt'), 'w')

if not reportfile:
    print 'unable to open report output: %s' % opath('report.txt')
    sys.exit()

msg("Report start: %s" % opath('report.txt'))
vmsg("Output Directory: %s" % (output_directory))

vmsg("Running ibnetdiscover")
(ret, out, err) = exec_opensm_to_string('flock -w 10 /var/run/cronquery /usr/sbin/ibnetdiscover --node-name-map /etc/ofa/node-name-map -p')
write_file(opath('ibnetdiscover.log'), out)
parse_ibnetdiscover_cables(out) 
vmsg('Ports found: %s' % (len(ports)));

#Determine the link speed, since ibdiagnet has inconsistent naming
diag_link_speed = args.ibspeed
if diag_link_speed == 'FDR':
    diag_link_speed = '14';
if diag_link_speed == 'EDR':
    diag_link_speed = '25'; 

vmsg("Running ibdiag")
exec_opensm_to_file(
	'flock -w 10 /var/run/cronquery /usr/bin/ibdiagnet -r -skip dup_node_desc -skip nodes_info -skip dup_guids -lw 4x -ls %s -P all=1 -pc -pm_pause_time %s --get_cable_info' 
	% (
	    diag_link_speed,
	    args.ibdiag_time
	), opath('ibdiag_stdout.txt'))
(ret, out, err) = exec_to_string ( ['/usr/bin/scp', 'root@%s:/var/tmp/ibdiagnet2/ibdiagnet2*' % args.sm, output_directory])
if ret != 0:
    msg('scp failed: %s' % err)
else:
    parse_ibdiagnet_csv('%s/ibdiagnet2.db_csv' % (output_directory));

#check on bad cables
find_underperforming_cables()

vmsg("Running ibnetdisover cache")
exec_opensm_to_file('flock -w 10 /var/run/cronquery /usr/sbin/ibnetdiscover --cache ibnetdiscover-cache', opath('ibnetdiscover_cache.log'))
 
#open and parse the ibdiagnet log
file = open(opath('ibdiagnet2.log'),'r')
parse_ibdiagnet(file.read())
file.close()

vmsg("Running SGI's cv2 tool")
(ret, out, err) = exec_to_string( ['/root/ibcv2/ibcv2', '-f', args.sgi_cv2_config], '/root/ibcv2') 
msg(out);
#msg(err);

msg("Report stop: %s" % opath('report.txt'))
reportfile.close() 
