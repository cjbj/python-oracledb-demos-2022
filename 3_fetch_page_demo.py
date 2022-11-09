# python-oracledb demo of tuning when fetching a "page" of rows.
#
# See https://python-oracledb.readthedocs.io/en/latest/user_guide/tuning.html

import os
import traceback

import oracledb

# Global settings:
# oracledb.defaults.prefetchrows = 2
# oracledb.defaults.arraysize    = 100

un = os.environ.get('PYTHON_USERNAME')
pw = os.environ.get('PYTHON_PASSWORD')
cs = os.environ.get('PYTHON_CONNECTSTRING')

def get_session_id(connection):
    sql = "SELECT sys_context('userenv','sid') FROM dual"
    result, = connection.cursor().execute(sql).fetchone()
    return result

def get_round_trips(connection):
    sql = """SELECT ss.value
             FROM v$sesstat ss, v$statname sn
             WHERE ss.sid = :sid
             AND ss.statistic# = sn.statistic#
             AND sn.name LIKE '%roundtrip%client%'"""
    system_conn = oracledb.connect(user="system", password="oracle", dsn=cs)
    round_trips, = system_conn.cursor().execute(sql, [sid]).fetchone()
    return round_trips

try:
    connection = oracledb.connect(user=un, password=pw, dsn=cs)

    #-------------------------------------------------------------------------------
    # Fetching a "fixed" number of rows

    NROWS = 20

    with connection.cursor() as cursor:

        sid = get_session_id(connection)  # user session id
        round_trips_before = get_round_trips(connection)

        cursor.arraysize    = NROWS         # default is 100
        cursor.prefetchrows = NROWS + 1     # default is 2

        sql = "select * from test offset 0 rows fetch next :r rows only"
        cursor.execute(sql, [NROWS])
        rows = cursor.fetchall()

        round_trips_after = get_round_trips(connection)

        print(f"\nfetching {len(rows)} rows with prefetchrows {cursor.prefetchrows} and arraysize {cursor.arraysize} takes {round_trips_after - round_trips_before} roundtrip(s)\n")

except oracledb.Error as e:
    error, = e.args
    traceback.print_tb(e.__traceback__)
    print(error.message)
