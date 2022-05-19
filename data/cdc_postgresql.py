import random
import time
from chinaName import create_name
from db_tools import *


def continue_cdc_data(table_name, for_num):
    connect = create_connect('all_field', db_type='postgresql')
    cursor = create_cursor(connect)
    for _ in range(int(for_num)):
        sql = 'select id from cdc_postgresql.'+table_name+' order by id desc limit 1'
        cursor.execute(sql)
        result = cursor.fetchall()
        if len(result) != 0:
            data_id = int(result[0][0]) + 1
        else:
            data_id = 1
        sql_insert = 'insert into cdc_postgresql.'+table_name+'(id,name,sex,createtime) VALUES(%s,%s,%s,%s)'
        val = (data_id, create_name(), random.choice(['F', 'M', 'U']), time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
        cursor.execute(sql_insert, val)
    connect.commit()
    close(connect, cursor)


def continue_cdc_data_2(table_name, for_num):
    connect = create_connect('all_field', db_type='postgresql')
    cursor = create_cursor(connect)
    for _ in range(int(for_num)):
        sql = 'select id from cdc_postgresql.'+table_name+' order by id desc limit 1'
        cursor.execute(sql)
        result = cursor.fetchall()
        if len(result) != 0:
            data_id = int(result[0][0]) + 1
        else:
            data_id = 1
        sql_insert = 'insert into cdc_postgresql.'+table_name+'(id,name,sex) VALUES(%s,%s,%s)'
        val = (data_id, create_name(), random.choice(['F', 'M', 'U']))
        cursor.execute(sql_insert, val)
    connect.commit()
    close(connect, cursor)


continue_cdc_data('cdc_q_1', 2)
# continue_cdc_data('cdc_key', 3)
# continue_cdc_data_2('cdc_pg', 2)
# continue_cdc_data('cdc_q_2_tz', 3)
