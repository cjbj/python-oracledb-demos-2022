# python-oracledb demo to show some different connection syntaxes with SQLAlchemy 1.4


#-------------------------------------------------------------------------------
# Snippet to use python-oracledb in SQLAlchemy 1.4
# (not needed in SQLAlchemy 2)

import oracledb
import sys
oracledb.version = "8.3.0"
sys.modules["cx_Oracle"] = oracledb

# end snippet
#-------------------------------------------------------------------------------

from sqlalchemy import create_engine
from sqlalchemy import text
import sys
import os

un = os.environ.get('PYTHON_USERNAME')
pw = os.environ.get('PYTHON_PASSWORD')
dsn = os.environ.get('PYTHON_CONNECTSTRING')

hostname, service_name = dsn.split("/")
port = 1521

#-------------------------------------------------------------------------------

def e1():
    engine = create_engine(f'oracle://{un}:{pw}@{hostname}:{port}/?service_name={service_name}')
    return engine

#-------------------------------------------------------------------------------

def e2():
    engine = create_engine(f'oracle://{un}:{pw}@',
                           connect_args={
                               "host": hostname,
                               "port": port,
                               "service_name": service_name
                               }
                            )
    return engine

#-------------------------------------------------------------------------------

def e3():
    engine = create_engine(f'oracle://{un}:{pw}@',
                           connect_args={
                               "dsn": dsn
                               # "dsn": "proddb"
                               # "dsn": "localhost:1522/orclpdb1?transport_connect_timeout=2"
                               }
                            )
    return engine

#-------------------------------------------------------------------------------

def e4():

    engine = create_engine(f'oracle://{un}:{pw}@',
                           connect_args={
                               "dsn": dsn,
                               "expire_time": 4,
                               "tcp_connect_timeout": 10
                               }
                            )
    return engine


#-------------------------------------------------------------------------------

engine = e4()
with engine.connect() as conn:
    print(conn.scalar(text("""SELECT UNIQUE CLIENT_DRIVER
                              FROM V$SESSION_CONNECT_INFO
                              WHERE SID = SYS_CONTEXT('USERENV', 'SID')""")))
