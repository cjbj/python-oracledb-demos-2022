# python-oracledb 'Thin mode' demo
#
# The thin mode doesn't replicate the Oracle Client NLS functionality but you
# can use Python support.  This also works in Thick mode.
#
# See https://python-oracledb.readthedocs.io/en/latest/user_guide/globalization.html

import locale
import os

import oracledb

conn = oracledb.connect(user=os.environ.get("PYTHON_USERNAME"),
                        password=os.environ.get("PYTHON_PASSWORD"),
                        dsn=os.environ.get("PYTHON_CONNECTSTRING"))
cursor = conn.cursor()

def query(cursor):
    cursor.execute("select sysdate from dual")
    d, = cursor.fetchone()
    print(d)

#-------------------------------------------------------------------------------

print()
print("No type handler")
query(cursor)

#-------------------------------------------------------------------------------

print("\nWith naive type handler")

def type_handler1(cursor, name, default_type, size, precision, scale):
    if default_type == oracledb.DB_TYPE_DATE:
        return cursor.var(default_type, arraysize=cursor.arraysize,
                outconverter=lambda v: v.strftime("%d-%m-%Y %H:%M"))

conn.outputtypehandler = type_handler1

query(cursor)

#-------------------------------------------------------------------------------

print("\nWith locale type handler")

locale.setlocale(locale.LC_ALL, 'de_DE.UTF-8')
locale_date_format = locale.nl_langinfo(locale.D_T_FMT)

def type_handler2(cursor, name, default_type, size, precision, scale):
    if default_type == oracledb.DB_TYPE_DATE:
        return cursor.var(default_type, arraysize=cursor.arraysize,
                outconverter=lambda v: v.strftime(locale_date_format))

conn.outputtypehandler = type_handler2

query(cursor)

print()
