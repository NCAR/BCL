#!/usr/bin/python
import sys
import syslog
import os

def vlog(level, string):
    vlevel = 3
    if 'VERBOSE' in os.environ:
	vlevel = int(os.environ['VERBOSE'])

    if vlevel >= level:
	print string

    if level < 3:
	syslog.syslog(string)

def elog(string):
    sys.stderr.write('%s\n' % (string))

def die_now(string):
    """ Print error and die right now """
    elog(string)
    sys.exit(1)
 
