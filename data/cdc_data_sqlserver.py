import random
import time
from chinaName import create_name
from db_tools import *


def continue_cdc_data(table_name, for_num=10000):
    connect = create_connect('datahub', db_type='sqlserver')
    cursor = create_cursor(connect)
    try:
        for _ in range(int(for_num)):
            sql = 'select top 1 id from cdc_test.'+table_name+' order by id desc'
            cursor.execute(sql)
            result = cursor.fetchall()
            if len(result) != 0:
                data_id = int(result[0][0]) + 1
            else:
                data_id = 1
            sql_insert = 'insert into cdc_test.'+table_name+'(id,name,sex,createtime) VALUES(%s,%s,%s,%s)'
            val = (data_id, create_name(), random.choice(['F', 'M', 'U']), time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
            cursor.execute(sql_insert, val)
            time.sleep(random.randint(10, 20))
            connect.commit()
    finally:
        close(connect, cursor)


continue_cdc_data('my_cdc')
