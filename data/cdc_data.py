import random
import time
from chinaName import create_name
from db_tools import *

connect = create_connect('datahub_cdc')
cursor = create_cursor(connect)


def continue_cdc_data():
    try:
        while True:
            for x in range(4, 5):
                for _ in range(0, random.randint(1, 1)):
                    sql = 'insert into datahub_cdc.cdc_'+str(x)+'(name,sex,createtime) VALUES(%s,%s,%s)'
                    val = (create_name(), random.choice(['F', 'M', 'U']), time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
                    cursor.execute(sql, val)
                    time.sleep(random.randint(1, 3))
                connect.commit()
    finally:
        close(connect, cursor)


def no_stop(sleep_time, *tab_name):
    try:
        while True:
            for x in tab_name:
                for _ in range(1):
                    sql = 'insert into datahub_cdc.' + x + '(name,sex,createtime) VALUES(%s,%s,%s)'
                    val = (create_name(), random.choice(['F', 'M', 'U']), time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
                    cursor.execute(sql, val)
                connect.commit()
            time.sleep(random.randint(1, int(sleep_time)))
    finally:
        connect.commit()
        close(connect, cursor)


def one_data(tab_name, data_num=10):
    for _ in range(int(data_num)):
        sql = 'insert into datahub_cdc.' + str(tab_name) + '(name,sex,createtime) VALUES(%s,%s,%s)'
        val = (create_name(), random.choice(['F', 'M', 'U']), time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
        cursor.execute(sql, val)
    connect.commit()


def test_basic():
    for _ in range(0, 1000):
        sql = 'insert into datahub_cdc.basic(name,sex,createtime) VALUES(%s,%s,%s)'
        val = (create_name(), random.choice(['F', 'M', 'U']), time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
        cursor.execute(sql, val)
    connect.commit()
    close(connect, cursor)


def big_data(tab_name):
    try:
        while True:
            sql = 'insert into '+tab_name+'(name,sex,createtime) select name,sex,now() from basic'
            cursor.execute(sql)
            connect.commit()
            time.sleep(1)
    finally:
        close(connect, cursor)


def big_data2():
    try:
        while True:
            sql = 'insert into cdc_4(name,sex,createtime) select name,sex,createtime from cdc_3'
            cursor.execute(sql)
            connect.commit()
            time.sleep(1)
    finally:
        close(connect, cursor)

# one_data('cdc_3', 2)
# continue_cdc_data()
# no_stop(20,'cdc_1', 'cdc_2', 'cdc_3')
no_stop(5, 'a1', 'a2', 'a3', 'a4')
# big_data('cdc_10')
# big_data2()


