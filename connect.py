import getpass
import os
import traceback
import oracledb

un = 'admin'
cs = '(description= (retry_count=20)(retry_delay=3)(address=(protocol=tcps)(port=1521)(host=adb.eu-madrid-1.oraclecloud.com))(connect_data=(service_name=g067633159c582f_dbmm_high.adb.oraclecloud.com))(security=(ssl_server_dn_match=yes)))'
pw = getpass.getpass(f'Enter password for {un}: ')

try:

    connection = oracledb.connect(user=un, password=pw, dsn=cs)

    with connection.cursor() as cursor:
        sql = """select systimestamp from dual"""
        for r, in cursor.execute(sql):
            print(r)

except oracledb.Error as e:
    error, = e.args
    print(error.message)
    traceback.print_tb(e.__traceback__)
