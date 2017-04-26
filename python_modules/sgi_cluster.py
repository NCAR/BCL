#!/usr/bin/python
#
# Filler module to get information about cluster
# TODO: clean this up and make it load from somewhere intelligently
#
import socket
import re
from nlog import vlog,die_now

def get_cluster_name():
    if re.search("^la", socket.gethostname()):
	return 'laramie'
    if re.search("^ch", socket.gethostname()):
	return 'cheyenne'
    return None

def get_cluster_name_formal():
    if re.search("^la", socket.gethostname()):
	return 'Laramie'
    if re.search("^ch", socket.gethostname()):
	return 'Cheyenne'
    return None
 
def is_sac():
    host = socket.gethostname()

    if host == 'lamgt' or host == 'chmgt':
	return True
    else:
	return False

def get_ice_info(node):
    m = re.search('^r([0-9]+)i([0-9]+)n([0-9]+)$', node) 

    if not m:
	return False

    return {
	'rack':	m.group(1),
	'lead':	'r{0}lead'.format(m.group(1)),
	'cmc':	'r{0}i{1}c'.format(m.group(1), m.group(2)),
	'iru':	m.group(2),
	'node':	m.group(3),
	'bmc':	'r{0}i{1}n{2}-bmc'.format(m.group(1), m.group(2), m.group(3))
    }

def get_lead(node):
    """ get lead but only from sac """
    if not is_sac():
	return False

    info = get_ice_info(node)
    if info:
	return info['lead']
    else:
	return socket.gethostname() 

def get_bmc(node):
    """ get node bmc name """
    
    return "{0}-bmc".format(node)
 
def get_sm():
    """ get smc nodes """
    
    host = socket.gethostname()

    if host == 'lamgt':
	return ['r1lead']
    elif host == 'chmgt':
	return ['r1lead', 'r2lead']
    return None

def get_ib_speed():
    """ get Infiniband network speed """
    
    host = socket.gethostname()

    if host == 'lamgt':
	return {'speed': 'EDR', 'link': 25, 'width': '4x'};
    elif host == 'chmgt':
	return {'speed': 'EDR', 'link': 25, 'width': '4x'};
    return None

def logical_to_physical(rack, iru):
    """ Convert SGI logical labels to physical labels """
    if iru > 4:
	rack *= 2
	iru -= 4
    else: #IRU in rack
	rack = rack * 2 + 1
    
    return {
	'rack': rack,
	'iru': iru
    }

def physical_to_logical(rack, iru):
    """ Convert SGI physical labels to logical labels """
    if rack & 1:
	#odd rack
	rack = (rack + 1) / 2
	iru += 4
    else: #even rack
	rack /= 2

    return {
	'rack': rack,
	'iru': iru
    }


def print_label(v, pformat = None):
    """ prints an sgi label from dict

    formats:
	raw: values as dict
	ibcv2: SGI ibcv2 format (r10i2s0c1.20)
	firmware: firmare label (r1i0s0 SW1 SwitchX -  Mellanox Technologies)
	physical: physical label (001IRU2-0-1-14)
	simple: simple label (r1i0s0 SW1/P1 or r1i3n17)
    
    """

    if pformat == 'raw':
	return str(v)
    elif pformat == 'ibcv2':
	return 'r%si%ss%sc%s.%s' % (
	    v['rack'],
	    v['iru'],
	    v['switch'],
	    v['switch_chip'],
	    v['port']
	)
    elif pformat == 'firmware':
	if not v['switch'] is None:
	    if v['port'] is None:
		return 'r%si%ss%s SW%s SwitchX -  Mellanox Technologies' % (
		    v['rack'],
		    v['iru'],
		    v['switch'],
		    v['switch_chip']
		)
	    else:
 		return 'r%si%ss%s SW%s SwitchX -  Mellanox Technologies/P%s' % (
		    v['rack'],
		    v['iru'],
		    v['switch'],
		    v['switch_chip'],
		    v['port']
		)
	elif not v['node'] is None:
 	    return 'r%si%ss%s/U%s/P%s' % (
		v['rack'],
		v['iru'],
		v['node'],
		v['hca'] if v['hca'] else 1, #default to first hca
		v['port'] if v['port'] else 1, #default to first port
	    ) 
    elif pformat == 'physical':
	if not v['port'] is None:
	    return '{0:0>3}IRU{1}-{2}-{3}-{4}'.format(
		v['rack'],
		v['iru'],
		v['switch'],
		v['switch_chip'],
		v['port']
	    )
	else:
	    return '{0:0>3}IRU{1}-{2}-{3}'.format(
		v['rack'],
		v['iru'],
		v['switch'],
		v['switch_chip']
	    ) 
    elif pformat == 'simple':
	if not v['node'] is None:
	    return 'r%si%sn%s' % (
		v['rack'],
		v['iru'],
		v['node']
	    ) 
	if not v['switch'] is None:
	    if not v['port'] is None:
		return 'r%si%ss%s SW%s/P%s' % (
		    v['rack'],
		    v['iru'],
		    v['switch'],
		    v['switch_chip'],
		    v['port']
		)  
	    else:
		return 'r%si%ss%s SW%s' % (
		    v['rack'],
		    v['iru'],
		    v['switch'],
		    v['switch_chip']
		) 
    vlog(1, 'unknown format: %s' % (pformat))	    
    return ''

def parse_label(label):
    """ Parse the sgi label names """

    def tint(v):
	if v:
	    return int(v)
	else:
	    return None

    vlog(5, 'parse_label(%s)' % (label))

    v = {
	'rack': None,
	'iru': None,
	'switch': None,
	'switch_chip': None,
	'node': None,
	'port': None,
	'hca': None,
    }

    #SGI ibcv2 format
    #r1i0s0c0.16
    #r9i2s0c1.20
    #r10i2s0c1.20
    r1 = re.compile(
	r"""
	r(?P<rack>[0-9]+)  #E-cell Rack - not E-Cell number
	i(?P<iru>[0-9]+)
	s(?P<switch>[0-9]+)
	c(?P<swchip>[0-9]+)
	\.
	(?P<port>[0-9]+)
	\s*
	""",
	re.VERBOSE
	)
    #r1i3n17/U1/P1
    r2 = re.compile(
	r"""
	r(?P<rack>[0-9]+)  #E-cell Rack - not E-Cell number
	i(?P<iru>[0-9]+)
	n(?P<node>[0-9]+)
	(?:
	    (?:/U(?P<hca>\d+)|)
	    (?:/P(?P<port>\d+)|)
	)
	\s*
	""",
	re.VERBOSE
	)
    #r1i0s0 SW1 SwitchX -  Mellanox Technologies
    #r1i0s0 SW0/P27
    r3 = re.compile(
	r"""
	r(?P<rack>[0-9]+)  #E-cell Rack - not E-Cell number
	i(?P<iru>[0-9]+)
	s(?P<switch>[0-9]+)
 	(?: 
	    \s+SW(?P<swchip>\d+)|
	) 
 	(?:
	    \s+SwitchX\s+\-+\s+Mellanox\ Technologies\s*|
	) 
 	(?:
	    /P(?P<port>\d+)|
	) 
	\s*
	""",
	re.VERBOSE
	)     
    #001IRU2-0-1-14 
    r4 = re.compile(
	r"""
	(?P<rack>[0-9]+)  #E-cell Rack - not E-Cell number
	IRU(?P<iru>[0-9]+)
	(?P<switch>[0-9]+)
	-
	(?P<swchip>\d+)
	-
	(?P<port>\d+)
	\s*
	""",
	re.VERBOSE
	)     
 
    match = r1.match(label) 
    if match:        
	v['rack'] = tint(match.group('rack'))
	v['iru'] = tint(match.group('iru'))
	v['switch'] = tint(match.group('switch'))
	v['switch_chip'] = tint(match.group('swchip'))
	v['port'] = tint(match.group('port'))
	return v
    match = r2.match(label) 
    if match:        
	v['rack'] = tint(match.group('rack'))
	v['iru'] = tint(match.group('iru'))
	v['node'] = tint(match.group('node'))
	v['port'] = tint(match.group('port'))
	v['hca'] = tint(match.group('hca'))
	return v
    match = r3.match(label) 
    if match:        
	v['rack'] = tint(match.group('rack'))
	v['iru'] = tint(match.group('iru'))
	v['switch'] = tint(match.group('switch'))
	v['switch_chip'] = tint(match.group('swchip'))
	v['port'] = tint(match.group('port'))
	return v    
    match = r4.match(label) 
    if match:        
	v['rack'] = tint(match.group('rack'))
	v['iru'] = tint(match.group('iru'))
	v['switch'] = tint(match.group('switch'))
	v['switch_chip'] = tint(match.group('swchip'))
	v['port'] = tint(match.group('port'))
	return v    
    return None

