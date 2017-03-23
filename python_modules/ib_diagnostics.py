#!/usr/bin/env python
from sys import path, argv
path.append("/ssg/bin/python_modules/") 
from nlog import vlog,die_now
from ClusterShell.NodeSet import NodeSet
from ClusterShell.Task import task_self
import re
import os
import csv
import cluster_info

#def exec_opensm_to_string ( cmd ):
#    """ Runs cmd on openSM host and places Return Value, STDOUT, STDERR into returned list  """
#    return exec_to_string (['/usr/bin/ssh','-n','-o','BatchMode=yes', args.sm, cmd] ) 

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
    name  = None
    hca   = None
    leaf  = None
    spine = None #only used for internal orca connections
    port  = None   

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
	vlog(5,'matched: %s' % match.group())
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
	#regex matches following: (new mlnx format if no label is known)
	#S7cfe900300bdf570/N7cfe900300bdf570/P28
	ib_portname_type3_regex = re.compile(
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
	match = ib_portname_type3_regex.match(label) 

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
	    vmsg(5, 'matched: %s' % match.group())
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

def register_cable ( ports, port1, port2 ):
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

def parse_ibnetdiscover_cables ( ports, contents ):
    """ Parse the output of 'ibnetdiscover -p' 

    Two types of line formats:
    CA    44  1 0x0002c9030045f121 4x FDR - SW     2 17 0x0002c903006e1430 ( 'localhost HCA-1' - 'MF0;js01ib2:SX60XX/U1' )
    SW     2 19 0x0002c903006e1430 4x SDR                                    'MF0;js01ib2:SX60XX/U1'
    SW    82 19 0x7cfe900300bdf4f0 4x ???                                    'r1i0s0 SW1'

    """
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
		register_cable(ports, port, None)
		#vlog(5, port)
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

		#vlog(5, port1)
		#vlog(5, port2)

		#cross reference connecting port
		port1['connection'] = port2
		port2['connection'] = port1
		register_cable(ports, port1, port2)
	else:
	    if line != "":
		vlog(3, 'Parse fail: %s' % line )
def msg_port_pretty_long ( port, why ): 
    """ msg port label with helpful info"""
    vlog(1,'%s: %s SPEED=%s LID=%s GUID=%s SN=%s PN=%s' % (
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

def find_underperforming_cables ( ports, issues, speed, width = "4x"):
    """ Checks all of the ports for any that are not at full width or speed or disabled """

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
	   vlog(5,'Localhost labeled port: %s <-> %s' % (port_pretty(port), 'N/A' if not port['connection'] else port_pretty(port['connection'])))
	   issues['label'].append({ 
	       'port': port,
	       'label': port['name']
	       })         

	if port['connection']: #ignore unconnected ports
	    if port['speed'] != speed:
	       issues['speed'].append({ 
		   'port': port,
		   'speed': port['speed']
		   })        
	    if port['width'] != width:
 	       issues['width'].append({ 
		   'port': port,
		   'width': port['width']
		   })        
	else: #check if unconnected ports are disabled
	    vlog(5, 'down port physstate:%s state:%s' % (port['PortPhyState'],port['PortState']))
	    if int(port['PortPhyState']) == 3: #physical state is disabled
                issues['disabled'].append({ 
		   'port': port
		   })        

def parse_ibdiagnet ( ports, issues, contents ):
    """ Parse the output of ibdiagnet """

    ibdiag_line_regex = re.compile(r"""
	    \s*-[^IW]-\s+	    #find all none Info and Warns
	    (?:
		(?!lid=0x[0-9a-z]+\ dev=\d+)		#ignore the lid dumps for counters since its dup
		(?P<msg>.*)				#extract message after type
	    )
	""", re.VERBOSE) 

    #Se41d2d03004bcfb0/Ne41d2d03004bcfb0/P20 - "port_rcv_remote_physical_errors" increased during the run (difference value=1,difference allowed threshold=1)
    #r9i1n24/U1/P1 - "port_rcv_remote_physical_errors" increased during the run (difference value=117,difference allowed threshold=1)
    ibdiag_line_regex_port = re.compile(r"""
	    ^\s*
	    (?P<port>\S*)
	    \s*-\s*" 
            (?P<counter>\S*)"\s*
            increased\ during\ the\ run\ \(difference\ value=
            (?P<value>[0-9]*),
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
		   vlog(4,'IBDiagnet2: %s: %s' % (match.group('label'), lmatch.group('msg')))

		   cmatch = ibdiag_line_regex_port.match(lmatch.group('msg'))
		   if cmatch:
		       issues['counters'].append({ 
			   'port': cmatch.group('port'),
			   'counter': cmatch.group('counter'),
			   'value': cmatch.group('value')
			   })
		   else:
		       if lmatch.group('msg') != 'Ports counters value Check finished with errors' \
			   and lmatch.group('msg') != 'Ports counters Difference Check (during run) finished with errors':
			       issues['unknown'].append('%s: %s' % (match.group('label'), lmatch.group('msg')))

def parse_ibdiagnet_csv ( ports, path_to_csv ):
    """ Parse the output of ibdiagnet ibdiagnet2.db_csv
	Limited to pulling the cable serials and state out currently
    """
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
			elif csv_mode == 'START_PORTS':
			    for port in ports:
				 if port['guid'] == rowdict['PortGuid'] and port['port'] == rowdict['PortNum']:
				     #just combine the data into the port
				     port.update(rowdict); 

                          
def find_cable_by_switch_leaf_port ( ports, name, leaf, port ):
    """ Checks all of the ports for any that are not at full width or speed """

    for port in ports:
	if port['name'] == name and port['leaf'] == leaf and port['port'] == port:
	    return port

    return None


