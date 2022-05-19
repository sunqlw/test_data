import pymysql
# import pymssql
import psycopg2
import cx_Oracle


def create_connect(db, db_type='mysql'):
    if db_type == 'mysql':
        connect = pymysql.connect(
            host="172.17.177.22",  # 数据库主机地址
            user="root",  # 数据库用户名
            password="bigData@123",  # 数据库密码
            database=db
        )
    elif db_type == 'tidb':
        connect = pymysql.connect(
            host="172.17.177.22",  # 数据库主机地址
            user="root",  # 数据库用户名
            password="123456",  # 数据库密码
            port=4000,
            database=db
        )
    elif db_type == 'oracle':
        connect = cx_Oracle.connect('SYSTEM', 'bigdata123', '172.17.177.22:1521/ORCLCDB')
    # elif db_type == 'sqlserver':
    #     connect = pymssql.connect(
    #         host="172.17.177.22",
    #         user="sa",  # 数据库用户名
    #         password="leap@123",  # 数据库密码
    #         database=db
    #     )
    elif db_type == 'postgresql':
        connect = psycopg2.connect(
            host="172.17.177.22",
            user="root",  # 数据库用户名
            password="hundsun",  # 数据库密码
            database=db
        )
    else:
        connect = None
    return connect


def create_cursor(connect):
    return connect.cursor()


def close(connect, cursor):
    cursor.close()
    connect.close()
