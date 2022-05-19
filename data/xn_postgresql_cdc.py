import random
import time
from chinaName import create_name
from db_tools import *

connect = create_connect('all_field', db_type='postgresql')
cursor = create_cursor(connect)


def continue_cdc_data(for_num, table_name, totals, sleep_time=1):
    data_id = 1
    sql = 'select id from cdc_postgresql.' + table_name + ' order by id desc limit 1'
    cursor.execute(sql)
    result = cursor.fetchall()
    if len(result) != 0:
        data_id = int(result[0][0]) + 1
    flag = 0
    while flag < for_num:
        for _ in range(int(totals)):
            sql_insert = 'insert into cdc_postgresql.' + table_name + '(id,name,sex,createtime) VALUES(%s,%s,%s,%s)'
            val = (data_id, create_name(), random.choice(['F', 'M', 'U']),
                   time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
            cursor.execute(sql_insert, val)
            data_id += 1
        connect.commit()
        time.sleep(sleep_time)
        flag += 1
    close(connect, cursor)


continue_cdc_data(100, 'cdc_q_1', 1, 10)
