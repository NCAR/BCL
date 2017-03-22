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
	
def write_file ( file_name, contents ):
    """ Takes file_name and writes contents to file. it will clobber file_name. """
    vlog(4, 'Writing File: %s SIZE=%s' % (file_name, len(contents)))
    file = open(file_name, 'w')
    file.write(contents)
    file.close()

