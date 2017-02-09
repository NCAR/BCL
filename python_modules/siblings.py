#!/usr/bin/env python
from sys import argv
import socket
import sys
import re
import os

nodes_per_blade = 2
slots_per_iru = 9

if re.search("^la", socket.gethostname()) is None:
    nodes_per_blade = 4
 
def node_to_tuple(n):
    m = re.match("([rR])([0-9]+)([iI])([0-9]+)([nN])([0-9]+)", n)
    if m is not None:
	#(rack, iru, node)
	return (int(m.group(2)), int(m.group(4)), int(m.group(6)))
    else:
	return None

def resolve_siblings(nodes):
    """ resolve out list of sibling nodes to given set of nodes """
    result = []
    for n in nodes:
	nt = node_to_tuple(n)
	for i in range(0,nodes_per_blade):
	    nid = (nt[2] % slots_per_iru) + (i*slots_per_iru)
	    nodename = "r%di%dn%d" % (nt[0], nt[1], nid)

	    if not nodename in result:
		result.append(nodename)

    return result

