import random
import time
from chinaName import create_name
from db_tools import *

connect = create_connect('datahub_cdc')
cursor = create_cursor(connect)


def create_tab(num):
    for x in range(10, int(num)+1):
        sql = "CREATE TABLE datahub_cdc_xn.a_cdc_"+str(x)+" (id bigint(20) auto_increment NOT NULL," \
              "name varchar(30) CHARACTER SET utf8 COLLATE utf8_general_ci NULL," \
              "sex varchar(10) CHARACTER SET utf8 COLLATE utf8_general_ci NULL," \
              "createtime datetime NULL COMMENT 'date类型的时间'," \
              "CONSTRAINT `PRIMARY` PRIMARY KEY (id)) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_general_ci COMMENT='';"
        cursor.execute(sql)
        connect.commit()
    close(connect, cursor)


def continue_date(for_time, tab_num, totals):
    flag = 0
    try:
        while flag < for_time:
            for x in range(1, int(tab_num) + 1):
                sql = "insert into datahub_cdc_xn.a_cdc_" + str(x) + \
                      "(name,sex,createtime) select name,sex,now() from datahub_cdc_xn.basic_"+str(totals)
                cursor.execute(sql)
            connect.commit()
            time.sleep(1)
            flag += 1
    finally:
        connect.commit()
        close(connect, cursor)


def one_data(for_time, sleep_time, *tab_name):
    flag = 0
    try:
        while flag < for_time:
            for x in tab_name:
                sql = "insert into datahub_cdc_xn." + x + "(name,sex,createtime) VALUES(%s,%s,%s)"
                val = (create_name(), random.choice(['F', 'M', 'U']), time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
                cursor.execute(sql, val)
            connect.commit()
            time.sleep(sleep_time)
            flag += 1
    finally:
        connect.commit()
        close(connect, cursor)


def truncate_table(start,end):
    for x in range(start, int(end)+1):
        sql = "truncate table datahub_cdc_xn.a_cdc_"+str(x)
        cursor.execute(sql)
        connect.commit()


# truncate_table(1, 10)
continue_date(1800, 1, 30000)
# one_data(100,5,'a_cdc_11')