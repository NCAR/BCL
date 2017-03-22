#!/usr/bin/python
#
# Filler module to get information about cluster
# TODO: clean this up and make it load from somewhere intelligently
#
import socket
import re

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
	'lead':	'r{}lead'.format(m.group(1)),
	'cmc':	'r{}i{}c'.format(m.group(1), m.group(2)),
	'iru':	m.group(2),
	'node':	m.group(3),
	'bmc':	'r{}i{}n{}-bmc'.format(m.group(1), m.group(2), m.group(3))
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
    
    return "{}-bmc".format(node)
 
def get_sm():
    """ get smc nodes """
    
    host = socket.gethostname()

    if host == 'lamgt':
	return ['r1lead']
    elif host == 'chmgt':
	return ['r1lead', 'r2lead']
 
