#!/usr/bin/python
from nlog import vlog

def read_file_first_line(filename):
    """ Read first line of given filename """
    result = None
    with open(filename, 'r') as f:
	result = f.readline()
	result = result.rstrip("\n")
	f.close()
    return result
	
