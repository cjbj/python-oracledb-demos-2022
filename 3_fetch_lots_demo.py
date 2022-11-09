# python-oracledb demo of tuning when fetching a large number of rows.
#
# See https://python-oracledb.readthedocs.io/en/latest/user_guide/tuning.html

import os
import time
import traceback

import oracledb

# Global settings
# oracledb.defaults.arraysize = 100

un = os.environ.get('PYTHON_USERNAME')
pw = os.environ.get('PYTHON_PASSWORD')
cs = os.environ.get('PYTHON_CONNECTSTRING')

try:
    connection = oracledb.connect(user=un, password=pw, dsn=cs)

    #-------------------------------------------------------------------------------
    # Fetching a "large" number of rows

    with connection.cursor() as cursor:

        start = time.time()

        cursor.arraysize = 10000

        sql = "select * from test"
        cursor.execute(sql)
        rows = cursor.fetchall()

        elapsed = round(time.time() - start, 3)
        print(f"\nfetching {len(rows)} rows with arraysize {cursor.arraysize} takes {elapsed} seconds\n")

except oracledb.Error as e:
    error, = e.args
    traceback.print_tb(e.__traceback__)
    print(error.message)
