# python-oracledb demo of what to do.  This is much faster than
# 2_load_demo_single.py
#
# See https://python-oracledb.readthedocs.io/en/latest/user_guide/batch_statement.html
#
# create table test (id number, job varchar2(40));
#

import traceback
import os
import csv
import time

import oracledb

un = os.environ.get('PYTHON_USERNAME')
pw = os.environ.get('PYTHON_PASSWORD')
cs = os.environ.get('PYTHON_CONNECTSTRING')

try:
    connection = oracledb.connect(user=un, password=pw, dsn=cs)

    with connection.cursor() as cursor:

        start = time.time()

        # Predefine the memory areas to match the table definition.
        cursor.setinputsizes(None, 40)

        with open('data.csv', 'r') as csv_file:
            csv_reader = csv.reader(csv_file, delimiter=',')
            data = []
            for line in csv_reader:
                data.append((line[0], line[1]))
            cursor.executemany(
                "insert into test (id, job) values (:1, :2)",
                data
            )
            connection.commit()

        elapsed = round(time.time() - start, 3)
        print(f"\n{elapsed} seconds for executemany()\n")

except oracledb.Error as e:
    error, = e.args
    traceback.print_tb(e.__traceback__)
    print(error.message)
