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
 
def to_hex(string):
    """ Dumps string as hex """

    if len(string):
	#https://stackoverflow.com/questions/12214801/print-a-string-as-hex-bytes
	return ":".join("{:02x}".format(ord(c)) for c in string)

    return None

def is_ascii(string):
    """ Determine is string only contains ascii characters """
    #https://stackoverflow.com/questions/196345/how-to-check-if-a-string-in-python-is-in-ascii
    return all(ord(c) < 128 for c in string)

