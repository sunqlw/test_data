import random
import time
from chinaName import create_name
from db_tools import *

connect = create_connect('C##DATAHUB', 'oracle')
cursor = create_cursor(connect)
sql = 'select * from C##DATAHUB.A_CDC'
cursor.execute(sql)
result = cursor.fetchall()
print(result)