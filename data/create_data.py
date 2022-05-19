import os


def create_csv_fields(fields_no, separator, field_prefix='test'):
    list_field = []
    for no in range(int(fields_no)):
        list_field.append(field_prefix+str(no))
    return separator.join(list_field)


def create_csv_data(file_name='test.txt', fields_no=10, separator=',', size=10):
    """
    :param file_name: 文件名，默认是test.txt
    :param fields_no: 字段数，默认是10
    :param separator: 分隔符，默认是,
    :param size: 文件大小，单位是M，默认是10M
    :return:
    """
    with open(file_name, 'a+', encoding='utf-8') as file:
        file_size = os.path.getsize(file_name)
        if file_size == 0:  # 等价于 if not file_size，如果文件大小为0，则表示要生成首列
            file.write(create_csv_fields(fields_no, separator))
            file.write('\n')
            file.flush()
            file_size = os.path.getsize(file_name)
        data_list = []
        for x in range(int(fields_no)):
            data_list.append('测试数据' + str(x))
        data_str = separator.join(data_list)
        str_size = len(data_str.encode())
        # 确定循环次数
        for _ in range((int(size)*1024*1024-file_size)//str_size):
            file.write(data_str)
            file.write('\n')
