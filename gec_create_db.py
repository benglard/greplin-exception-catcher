#!/usr/bin/env python

import MySQLdb as mdb
import sys
import getopt

""" gec_create_db.py

Usage: python gec_create_db.py server_ip_address username password database_name
1.If database_name not provided, a database will be created with the name: 'Exception-Catcher',
and tables will be created.
2.If database_name provided but connect() fails, a database will be created with the name provided,
and tables will be created.
3. If all four parameters provided and connect() succeeds, will proceed to create tables.

"""

"""
Tables Created:
    1) errors - list of errors
    2) errorEnvironments - list of environments where errors occur
    3) errorServers - list of servers where errors occur
"""

"""
NOTE: In some scenarios the following error message appears: 'Error 1049: Unknown database...'.
This is not an issue. The database and tables are still created correctly.
"""

def __db_call(con, action, params = None):
    print "dbcall -- %s %s" % (action, params)
    cur = con.cursor()
    if params != None:
        cur.execute(action, params)
    else:
        cur.execute(action)
    result = cur.fetchall()
    con.commit
    cur.close
    return result

def __init_db(server, user, password, db):
    try:
        con = mdb.connect(host = server, user = user, passwd = password, db = db)
        __db_call(con, "CREATE TABLE IF NOT EXISTS errors "
                       "(project VARCHAR(500), backtrace TEXT, type VARCHAR(500), hash VARCHAR(500), "
                       "active TINYINT(1), count INT, errorLevel VARCHAR(500) DEFAULT 'error', "
                       "firstOccurence DATETIME, lastOccurence DATETIME, lastMessage VARCHAR(500), "
                       "environments VARCHAR(500), servers VARCHAR(500), PRIMARY KEY (project))")
        __db_call(con, "CREATE TABLE IF NOT EXISTS errorEnvironments "
                  "(project VARCHAR(500), environments VARCHAR(500), "
                  "FOREIGN KEY (project) REFERENCES errors(project))")
        __db_call(con, "CREATE TABLE IF NOT EXISTS errorServers "
                  "(project VARCHAR(500), servers VARCHAR(500), "
                  "FOREIGN KEY (project) REFERENCES errors(project))")
        con.close()
    except mdb.Error, e:
        try:
            __create_db(server = server, user = user, password = password, dbName = db)
        except mdb.Error, e:
            print "Error %d: %s" % (e.args[0], e.args[1])
        print "Error %d: %s" % (e.args[0], e.args[1])

def __create_db(server, user, password, dbName): 
    try: 
        db=mdb.connect(host = 'localhost', user='root', passwd = password)
        c=db.cursor()
        cmd = "CREATE DATABASE " + dbName + ";"
        c.execute(cmd)
        try:
            __init_db(server, user, password, dbName)
        except mdb.Error, e:
            print "Error %d: %s" % (e.args[0], e.args[1])
    except mdb.Error, e:
        print "Error %d: %s" % (e.args[0], e.args[1])

def usage():
    print "python gec_create_db.py server username password db name."
def usage2():
    print "If database with given name does not exist, it will be created for you."
    print "If db name not provided, a database will be created with name: 'Exception_Catcher'"

def main():
    try:
        opts = (sys.argv)
    except getopt.GetoptError, err:
        usage()
        usage2()
        sys.exit()
    server = opts[1]
    username = opts[2]
    password = opts[3]
    
    if len(opts) == 4:
        usage2()
        __create_db(server = server, user = username, password = password, dbName = 'Exception_Catcher')
    else:
        dbName = opts[4]
        __init_db(server = server, user = username, password = password, db = dbName)

if __name__ == '__main__':
    main()
