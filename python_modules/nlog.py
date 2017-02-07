#!/usr/bin/python
import sys

def vlog(level, string):
    if verbose >= level:
	print string

def elog(string):
    sys.stderr.write('%s\n' % (string))

def die_now(string):
    """ Print error and die right now """
    elog(string)
    sys.exit(1)
 
