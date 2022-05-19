# 该方法用于创建任意字段数任意大小的分隔符文件
import os
import time
from create_data import create_csv_data


print("开始生成文件：")
start = time.time()
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
fields = ['10', '30', '50', '100', '150', '200']
sizes = ['5', '10', '50', '100', '300', '490']
for field in fields:
    for size in sizes:
        file_name = field+'个字段_'+size+'M.txt'
        print("开始生成文件：", file_name)
        file_path = os.path.join(BASE_DIR, 'data_file', 'csv', '性能测试', file_name)
        create_csv_data(file_name=file_path, fields_no=field, size=size)
end = time.time()
print("总共耗时：", end-start)

