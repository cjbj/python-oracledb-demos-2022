# python-oracledb demo of fetching LOBs.
#
# See https://python-oracledb.readthedocs.io/en/latest/user_guide/lob_data.html

import os
import traceback

import oracledb

un = os.environ.get('PYTHON_USERNAME')
pw = os.environ.get('PYTHON_PASSWORD')
cs = os.environ.get('PYTHON_CONNECTSTRING')

#
# !! No longer needed but still works.  Can now use oracledb.defaults.fetch_lobs instead !!
#
# def output_type_handler(cursor, name, default_type, size, precision, scale):
#     if default_type == oracledb.CLOB:
#         return cursor.var(oracledb.LONG_STRING, arraysize=cursor.arraysize)
#     if default_type == oracledb.BLOB:
#         return cursor.var(oracledb.LONG_BINARY, arraysize=cursor.arraysize)
#
# connection.outputtypehandler = output_type_handler

try:
    connection = oracledb.connect(user=un, password=pw, dsn=cs)

    with connection.cursor() as cursor:

        #-------------------------------------------------------------------------------
        # Fetching as LOB objects

        oracledb.defaults.fetch_lobs = True  # (the default)
        print("\noracledb.defaults.fetch_lobs is:", oracledb.defaults.fetch_lobs)

        sql = "select to_clob('A big string') from dual"
        cursor.execute(sql)
        r, = cursor.fetchone()
        type = cursor.description[0][1]
        print("Type is:", type)
        data = r.read()
        print("Data is:", data)

        #-------------------------------------------------------------------------------
        # Fetching directly as a string is much faster but limited to 1 GB

        oracledb.defaults.fetch_lobs = False
        print("\noracledb.defaults.fetch_lobs is:", oracledb.defaults.fetch_lobs)

        sql = "select to_clob('Another big string') from dual"
        cursor.execute(sql)
        r, = cursor.fetchone()
        type = cursor.description[0][1]
        print("Type is:", type)
        data = r
        print("Data is:", r)
        print()

except oracledb.Error as e:
    error, = e.args
    traceback.print_tb(e.__traceback__)
    print(error.message)
