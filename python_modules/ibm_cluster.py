#!/usr/bin/python
#
# Filler module to get information about cluster
# TODO: clean this up and make it load from somewhere intelligently
#
import socket
import re

def get_cluster_name():
    if re.search("^ys", socket.gethostname()):
	return 'yellowstone'
    elif re.search("^js", socket.gethostname()):
	return 'jellystone'
    elif re.search("^er", socket.gethostname()):
	return 'erebus'
    return None

def get_cluster_name_formal():
    if re.search("^ys", socket.gethostname()):
	return 'Yellowstone'
    elif re.search("^js", socket.gethostname()):
	return 'Jellystone'
    elif re.search("^er", socket.gethostname()):
	return 'Erebus'
    return None        
 
def is_xcat_mgr():
    host = socket.gethostname()

    if host == 'ysmgt1' or host == 'ermgt1' or host == 'jsmgt1':
	return True
    else:
	return False
  
def get_sm():
    """ get smc nodes """
    
    host = socket.gethostname()

    if host == 'lamgt1':
	return ['laufm1', 'laufm2']
    elif host == 'ysmgt1':
	return ['ysufm1', 'ysufm2']
    elif host == 'ermgt1':
	return ['erufm1', 'erufm2']
    return None                    

def get_bmc(node):
    """ get node bmc name """
    
    return "{0}-imm".format(node)

def get_ib_speed():
    """ get Infiniband network speed """
    
    host = socket.gethostname()

    if host == 'ysmgt1':
	return {'speed': 'FDR', 'link': 14, 'width': '4x'};
    elif host == 'ermgt1':
	return {'speed': 'FDR10', 'link': 10, 'width': '4x'};
    elif host == 'jsmgt1':
	return {'speed': 'FDR', 'link': 14, 'width': '4x'};
    return None
 
