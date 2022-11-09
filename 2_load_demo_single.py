# python-oracledb demo of what NOT to do.  This is slow.
# Use 2_load_demo_batch.py instead.
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

        with open('data.csv', 'r') as csv_file:
            csv_reader = csv.reader(csv_file, delimiter=',')
            for line in csv_reader:
                cursor.execute(
                    "insert into test (id, job) values (:1, :2)",
                    [line[0], line[1]]
                )
            connection.commit()

        elapsed = round(time.time() - start, 3)
        print(f"\n{elapsed} seconds for execute()\n")

except oracledb.Error as e:
    error, = e.args
    traceback.print_tb(e.__traceback__)
    print(error.message)
