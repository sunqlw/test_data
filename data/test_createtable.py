import random
import time
from chinaName import create_name
from db_tools import *

connect = create_connect('datahub_wide')
cursor = create_cursor(connect)


def create_wide_table(table_name, field_num):
    try:
        sql = 'CREATE TABLE if not exists`' + table_name + '`(`id` bigint(20) NOT NULL AUTO_INCREMENT,'
        for x in range(1, int(field_num)):
            sql += '`test' + str(x) + '` varchar(10) DEFAULT NULL,'
        sql += 'PRIMARY KEY (`id`)) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8'
        print(sql)
        cursor.execute(sql)
    finally:
        connect.commit()
        close(connect, cursor)


def insert_data(table_name):
    try:
        while True:
            time.sleep(10)
            sql = 'SELECT COUNT(1) FROM information_schema.COLUMNS WHERE table_schema = "datahub_wide" AND table_name = "' + table_name + '"'
            cursor.execute(sql)
            result = cursor.fetchall()
            field_num = result[0][0]
            sql = 'select id from datahub_wide.' + table_name + ' order by id desc limit 1'
            cursor.execute(sql)
            result_2 = cursor.fetchall()
            if len(result_2) != 0:
                data_id = int(result_2[0][0]) + 1
            else:
                data_id = 1
            val = [str(data_id)]
            for x in range(1, field_num):
                val.append('"t' + str(data_id) + str(x)+'"')
            val_str = ','.join(val)
            sql_insert = 'insert into ' + table_name + ' VALUES(' + val_str + ')'
            cursor.execute(sql_insert)
            connect.commit()
    finally:
        connect.commit()
        close(connect, cursor)


# create_wide_table('test1', 200)
insert_data('test1')