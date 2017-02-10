#!/usr/bin/python
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

