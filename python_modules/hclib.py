#!/usr/bin/python
import os, time
import subprocess
from sys import path, argv                                                                                                                                        
import commands

def get_fsspace(mnt):
	return int(commands.getoutput('df {0}'.format(mnt)).split('\n')[1].split()[4][:-1])

#def get_fsspace(mnt):
#	used = commands.getoutput('df {0}'.format(mnt)).split('\n')[1].split()[4][:-1]
#	return int(used)

def get_memory():
        totalMemory = os.popen("free -g").readlines()[1].split()[1]
        return int(totalMemory)

def cpuTest():
	cpuCount = os.sysconf(os.sysconf_names["SC_NPROCESSORS_ONLN"])
	return int(cpuCount)

def is_ibstat_ok():
	out = os.popen("ibstat").read()
	if "State: Active" in out and "Physical state: LinkUp" in out and "Rate: 100" in out:
		return True
	else:
		return False

def is_service_running(svc):
	with open(os.devnull, 'wb') as hide_output:
		exit_code = subprocess.Popen(['systemctl', 'status', svc], stdout=hide_output, stderr=hide_output).wait()
		return exit_code == 0

def is_mounted(mnt):
	mounted = os.path.ismount(mnt)
	return mounted

def file_exists(file):
	if os.path.isfile(file):
		return True
	else:
		return False
	
