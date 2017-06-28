#!/usr/bin/python
import sqlite3
from nlog import vlog,die_now

def init(database_path):
    """ Attempt to open a SQLite database and returns the connection and cursor """
    try:
	SQL_CONNECTION = sqlite3.connect(database_path, isolation_level=None)
	SQL_CONNECTION.row_factory = sqlite3.Row
	SQL = SQL_CONNECTION.cursor()
	return (SQL_CONNECTION, SQL)
    except Exception as err:
	vlog(1, 'Unable to Open DB: {0}'.format(err))

    return None
 
def close(SQL_CONNECTION, SQL):
    """ Safely close a sqlite db conneciton """
    SQL.close()
    SQL_CONNECTION.close()
 
def add_column(SQL, table, column, column_type):
    """ Add a column to sqlite table if it is missing 
    this is the easy way to update tables for new features
    """

    SQL.execute('PRAGMA table_info(%s);' % (table))

    for row in SQL.fetchall():
	if row['name'] == column:
	    return True #already exists

    #SQL.execute('ALTER TABLE ? ADD ? ?;', (table, column, column_type))
    SQL.execute('ALTER TABLE %s ADD %s %s;' % (table, column, column_type))

    return True
 

