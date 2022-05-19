from chinaName import create_name
from db_tools import *


def create_tables(db_name, tab_name, tab_num, fields):
    """
    根据数据库名，表前缀，表个数，字段数批量创建表
    :param db_name: 数据库名
    :param tab_name: 表前缀
    :param tab_num: 表个数
    :param fields: 字段数
    :return:
    """
    connect = create_connect(db_name)
    cursor = create_cursor(connect)
    for x in range(1, int(tab_num)+1):
        sql = 'CREATE TABLE if not exists`' + tab_name + str(x).zfill(3) + \
              '`(`id` bigint(20) NOT NULL AUTO_INCREMENT,`name` varchar(10) DEFAULT NULL,`sex` varchar(1) DEFAULT NULL,'
        for y in range(1, int(fields)-2):
            sql += '`field' + str(y).zfill(3) + '` varchar(10) DEFAULT NULL,'
        sql += 'PRIMARY KEY (`id`)) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8'
        print(sql)
        cursor.execute(sql)
        connect.commit()
    close(connect, cursor)


create_tables('datahub_wide', 'sqlw', 1, 2)


