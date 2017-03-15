#!/usr/bin/env python
import os
import time
import fcntl
from nlog import vlog,die_now

def _try_lock_once(fd):
    """ Try to get lock once """
    try:
	ret = fcntl.flock(fd.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
	vlog(5, 'flock ret: {}'.format(ret))
	if ret == None:
	    return True
    except Exception as exp:
	vlog(5, 'flock exception: {}'.format(exp))
	return False
    except:
	return False

    return False

def try_lock(file_path, tries = 5):
    """ Open file and try to get lock tries times 
	The file_descriptor must remain in scope for lock to hold
    """
    try:
	file_descriptor = open(file_path, 'a')
    except Exception as exp:
	vlog(5, 'unable to open {} with exception: {}'.format(file_path, exp))
	return False

    for x in xrange(0, tries):
	if _try_lock_once(file_descriptor):
	    vlog(5, 'lock obtained')
	    return file_descriptor 
	elif x != tries: 
	    vlog(4, 'attemping {} of {} to get lock failed. retrying in {} seconds.'.format(x, tries, x))
	    time.sleep(x)     

    return False

