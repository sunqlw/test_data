import random
import time
from chinaName import create_name
from db_tools import *

connect = create_connect('datahub', db_type='tidb')
cursor = create_cursor(connect)


def offset_date(*tab_name):
    try:
        while True:
            for x in tab_name:
                sql = 'insert into datahub.' + x + '(name,sex,createtime) VALUES(%s,%s,%s)'
                val = (create_name(), random.choice(['F', 'M', 'U']), time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
                cursor.execute(sql, val)
                time.sleep(random.randint(5, 10))
                connect.commit()
    finally:
        close(connect, cursor)


offset_date('tidb_offset')
