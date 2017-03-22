#!/usr/bin/python
#
# Filler module to get information about cluster
# TODO: clean this up and make it load from somewhere intelligently
#
from sys import path, argv
path.append("/ssg/bin/python_modules/")
import sgi_cluster
import socket
import re

def get_cluster_name():
    return sgi_cluster.get_cluster_name()

def get_cluster_name_formal():
    return sgi_cluster.get_cluster_name_formal()

def get_bmc(node):
    """ get node bmc name """
    return sgi_cluster.get_bmc()
 
def get_sm():
    """ get smc nodes """
    return sgi_cluster.get_sm()
    
def is_mgr():
    """ Is this node the cluster manager """
    return sgi_cluster.is_sac()

