#!/usr/bin/python
import sys
import syslog

def vlog(level, string):
    #global VERBOSE
    #if VERBOSE >= level:
    print string
    if level < 3:
	syslog.syslog(string)

def elog(string):
    sys.stderr.write('%s\n' % (string))

def die_now(string):
    """ Print error and die right now """
    elog(string)
    sys.exit(1)
 
