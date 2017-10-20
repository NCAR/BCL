#!/usr/bin/python
import os
import json


def file_exists(file):
	if os.path.isfile(file):
		return True

def validate_json(file):
	if file_exists(file):
		try:
			json.loads(open(file, "r").read())
		except ValueError as e:
			return ('ERROR: %s' % e)
		else: return True
	else: return ('ERROR: %s does not exist' % file)

