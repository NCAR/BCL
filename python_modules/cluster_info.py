#!/usr/bin/python
#
# Filler module to get information about cluster
# TODO: clean this up and make it load from somewhere intelligently
#
from sys import path, argv
path.append("/ssg/bin/python_modules/")
import sgi_cluster
import ibm_cluster
import socket
import re

def get_cluster_info():
    if re.search("^(la|ch)", socket.gethostname()):
	return {
	    'vendor':	'sgi',
	    'type':	'icexa'

	};
    if re.search("^(ys|er|js)", socket.gethostname()):
	return {
	    'vendor':	'ibm',
	    'type':	'1410' #idataplex

	};                   
    return None

def get_cluster_type():
    i = get_cluster_info()
    if not i:
	return None

    return i['type']

def get_cluster_vendor():
    i = get_cluster_info()
    if not i:
	return None

    return i['vendor']

def get_cluster_name():
    if get_cluster_vendor() == "sgi":
	return sgi_cluster.get_cluster_name()
    elif get_cluster_vendor() == "ibm": 
	return ibm_cluster.get_cluster_name()
    return None 

def get_cluster_name_formal():
    if get_cluster_vendor() == "sgi":
	return sgi_cluster.get_cluster_name_formal()
    elif get_cluster_vendor() == "ibm": 
	return ibm_cluster.get_cluster_name_formal()
    return None

def get_bmc(node):
    """ get node bmc name """
    if get_cluster_vendor() == "sgi":
	return sgi_cluster.get_bmc()
    elif get_cluster_vendor() == "ibm": 
	return ibm_cluster.get_bmc()
    return None
 
def get_sm():
    """ get smc nodes """
    if get_cluster_vendor() == "sgi":
	return sgi_cluster.get_sm()
    elif get_cluster_vendor() == "ibm": 
	return ibm_cluster.get_sm()
    return None

def get_ib_speed():
    """ get Infiniband network speed """
    if get_cluster_vendor() == "sgi":
	return sgi_cluster.get_ib_speed()
    elif get_cluster_vendor() == "ibm": 
	return ibm_cluster.get_ib_speed()
    return None
    
def is_mgr():
    """ Is this node the cluster manager """
    if get_cluster_vendor() == "sgi":
	return sgi_cluster.is_sac()
    elif get_cluster_vendor() == "ibm": 
	return ibm_cluster.is_xcat_mgr()

    return False

