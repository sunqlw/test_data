# 批量建表语句
def test_create_table(self):
    connect = self.create_connect('datahub_xn_50')
    cursor = self.create_cursor(connect)
    for x in range(101, 102):
        sql = 'CREATE TABLE `xn_50_' + str(x) + \
              '`(`id` int(11) NOT NULL AUTO_INCREMENT,`name` varchar(5) DEFAULT NULL,`sex` varchar(1) DEFAULT NULL,' \
              '`age` int(11) DEFAULT NULL,`address` varchar(20) DEFAULT NULL,`father` varchar(5) DEFAULT NULL,' \
              '`mother` varchar(5) DEFAULT NULL,`tel` varchar(11) DEFAULT NULL,`city` varchar(5) DEFAULT NULL,' \
              'PRIMARY KEY (`id`)) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8'
        cursor.execute(sql)
        connect.commit()
    self.close(connect, cursor)


# 批量建表语句
def test_create_table_50_field(self):
    connect = self.create_connect('datahub_xn_50')
    cursor = self.create_cursor(connect)
    sql = '`(`id` int(11) NOT NULL AUTO_INCREMENT,'
    for x in range(1, 50):
        sql += '`field' + str(x) + '` varchar(50) DEFAULT NULL,'
    # print(sql)
    for y in range(150, 201):
        sql_exc = 'CREATE TABLE `xn_50field_' + str(y) + sql + \
                  'PRIMARY KEY (`id`)) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8'
        cursor.execute(sql_exc)
        connect.commit()
    self.close(connect, cursor)


# 往datahub_more库里面批量增加数据
def test_insert_50_field_data(self):
    connect = self.create_connect('datahub_xn_50')
    cursor = self.create_cursor(connect)
    for x in range(101, 150):
        for y in range(random.randint(1, 3)):  # 控制数据条数
            int_list = random.sample(range(1, 49), 3)  # 生成三个不相同的随机数
            str1 = str(int_list[0])
            str2 = str(int_list[1])
            str3 = str(int_list[2])
            sql = "insert into xn_50field_" + str(x) + \
                  "(field" + str1 + ",field" + str2 + ",field" + str3 + ") VALUES(%s, %s, %s)"
            val = ('test' + str1, 'test' + str2, 'test' + str3)
            cursor.execute(sql, val)
        connect.commit()
    self.close(connect, cursor)


def test_continue_data(self):
    connect = self.create_connect('datahub')
    cursor = self.create_cursor(connect)
    while True:
        time.sleep(2)
        now = time.localtime()
        sql = "insert into zl_test(name,age,createtime) VALUES(%s, %s, %s)"
        val = (create_name(), random.randint(16, 25), time.strftime("%Y-%m-%d %H:%M:%S", now))
        cursor.execute(sql, val)
        connect.commit()
    self.close(connect, cursor)


def test_continue_data_2(self):
    connect = self.create_connect('datahub_xn')
    cursor = self.create_cursor(connect)
    x = 0
    while True:
        x += 1
        now = time.localtime()
        sql = "insert into kw_datas_2(name,sex,createtime) VALUES(%s, %s, %s)"
        val = (create_name(), random.choice(['F', 'M', 'U', 'N']), time.strftime("%Y-%m-%d %H:%M:%S", now))
        cursor.execute(sql, val)
        if x % 100 == 0:
            connect.commit()
    self.close(connect, cursor)


def test_continue_data_3(self):
    connect = self.create_connect('datahub')
    cursor = self.create_cursor(connect)
    # for x in range(random.randint(1, 10)):
    while True:
        time.sleep(1)
        now = time.localtime()
        sql = "insert into a_data_insignt(name,sex,age,class,city,grade,createtime) VALUES(%s,%s,%s,%s,%s,%s,%s)"
        val = (create_name(), random.choice(['F', 'M']), random.randint(12, 18),
               random.choice(['1班', '2班', '3班', '4班']),
               random.choice(['高新区', '天府新区', '双流区', '武侯区', '金牛区', '成华区', '青羊区', '温江区']),
               random.randint(60, 100), now)
        cursor.execute(sql, val)
        connect.commit()
    self.close(connect, cursor)


def test_continue_data_4(self):
    connect = self.create_connect('datahub')
    cursor = self.create_cursor(connect)
    # for x in range(random.randint(1, 10)):
    while True:
        time.sleep(1)
        sql = "insert into a_grades_tab(english,math,china) VALUES(%s,%s,%s)"
        val = (random.randint(0, 101), random.randint(0, 101), random.randint(0, 101))
        cursor.execute(sql, val)
        connect.commit()
    self.close(connect, cursor)


# 批量建表语句
def test_create_table_100_field(self):
    connect = self.create_connect('datahub_xn_100')
    cursor = self.create_cursor(connect)
    sql = '`(`id` int(11) NOT NULL AUTO_INCREMENT,'
    for x in range(1, 100):
        sql += '`field' + str(x) + '` varchar(50) DEFAULT NULL,'
    # print(sql)
    for y in range(101, 251):
        sql_exc = 'CREATE TABLE `xn_100field_' + str(y) + sql + \
                  'PRIMARY KEY (`id`)) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8'
        cursor.execute(sql_exc)
        connect.commit()
    self.close(connect, cursor)


def test_insert1(self):
    connect = self.create_connect('datahub_xn')
    cursor = self.create_cursor(connect)
    for x in range(30):
        sql = "insert into bw_data(name,sex,createtime) select name,sex,createtime from kw_datas_2"
        cursor.execute(sql)
    connect.commit()
    self.close(connect, cursor)


# 往datahub_more库里面批量增加数据
def test_insert_100_field_data(self):
    connect = self.create_connect('datahub_xn_100')
    cursor = self.create_cursor(connect)
    for x in range(101, 200):
        for y in range(random.randint(1, 3)):  # 控制数据条数
            int_list = random.sample(range(1, 99), 3)  # 生成三个不相同的随机数
            str1 = str(int_list[0])
            str2 = str(int_list[1])
            str3 = str(int_list[2])
            sql = "insert into xn_100field_" + str(x) + \
                  "(field" + str1 + ",field" + str2 + ",field" + str3 + ") VALUES(%s, %s, %s)"
            val = ('test' + str1, 'test' + str2, 'test' + str3)
            cursor.execute(sql, val)
        connect.commit()
    self.close(connect, cursor)


def test_create_table_2(self):
    connect = self.create_connect('datahub_xn_50')
    cursor = self.create_cursor(connect)
    for x in range(1, 2):
        sql = 'CREATE TABLE `personal_info_' + str(x) + \
              '`(`id` int(11) NOT NULL AUTO_INCREMENT,`name` varchar(5) DEFAULT NULL,`sex` varchar(1) DEFAULT NULL,' \
              '`age` int(11) DEFAULT NULL,' \
              'PRIMARY KEY (`id`)) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8'
        cursor.execute(sql)
        connect.commit()
    self.close(connect, cursor)


# 往datahub_more库里面批量增加数据
def test_insert_data_1(self):
    connect = self.create_connect('datahub_more')
    cursor = self.create_cursor(connect)
    for x in range(1, 2):
        for y in range(random.randint(1, 100)):
            sql = "insert into personal_info_" + str(x) + \
                  "(name,sex,age) VALUES(%s, %s, %s)"
            val = (create_name(), random.choice(['F', 'M', 'U', 'N']), random.randint(16, 25))
            cursor.execute(sql, val)
        connect.commit()
    self.close(connect, cursor)


def test_insert_data_1(self):
    connect = self.create_connect('datahub_more')
    cursor = self.create_cursor(connect)
    for x in range(1, 2):
        for y in range(random.randint(1, 100)):
            sql = "insert into personal_info_" + str(x) + \
                  "(name,sex,age) VALUES(%s, %s, %s)"
            val = (create_name(), random.choice(['F', 'M', 'U', 'N']), random.randint(16, 25))
            cursor.execute(sql, val)
        connect.commit()
    self.close(connect, cursor)


# 往datahub_zl_more库里面批量增加数据
def test_insert_data_2(self):
    connect = self.create_connect('datahub_zl_more')
    cursor = self.create_cursor(connect)
    for x in range(100, 126):
        for y in range(random.randint(1, 100)):
            sql = "insert into student_" + str(x) + \
                  "(name,sex,age,address,father,mother,tel,city) VALUES(%s, %s, %s, %s, %s, %s, %s, %s)"
            val = (create_name(), random.choice(['F', 'M']), random.randint(16, 25), '高新区软件园C12', create_name(),
                   create_name(), '1838046192', '成都')
            cursor.execute(sql, val)
            connect.commit()
    self.close(connect, cursor)


def test_insert_data_2_1(self):
    connect = self.create_connect('datahub_zl_more')
    cursor = self.create_cursor(connect)
    for x in range(1001, 1110):
        sql = "insert into student_" + str(x) + " select * from student_11"
        cursor.execute(sql)
        connect.commit()
    self.close(connect, cursor)


# 往datahub_zl库里面增加数据
def test_insert_data_3(self):
    connect = self.create_connect('datahub_zl')
    cursor = self.create_cursor(connect)
    # for x in range(random.randint(1, 10)):
    while True:
        sql = "insert into a_zl_name(name) VALUES(%s)"
        val = (create_name())
        time.sleep(random.randint(8, 15))
        cursor.execute(sql, val)
        connect.commit()


def test_insert_data_3_1(self):
    connect = self.create_connect('datahub_zl')
    cursor = self.create_cursor(connect)
    for x in range(random.randint(1, 10)):
        # while True:
        sql = "insert into zl_error_flink(name) VALUES(%s)"
        val = (create_name())
        time.sleep(random.randint(1, 3))
        cursor.execute(sql, val)
        connect.commit()


def test_insert_data_4(self):
    connect = self.create_connect('datahub_xn')
    cursor = self.create_cursor(connect)
    # for x in range(22, 22):
    i = 1
    while True:
        sql = "insert into xn_1(name,sex) VALUES(%s,%s)"
        val = (create_name(), random.choice(['F', 'M']))
        cursor.execute(sql, val)
        i += 1
        if i % 100 == 0:
            connect.commit()


def test_insert_data_5(self):
    connect = self.create_connect('datahub_zl')
    cursor = self.create_cursor(connect)
    # i = 1
    while True:
        sql = "insert into a_zl_name(name) VALUES(%s)"
        val = (create_name())
        cursor.execute(sql, val)
        connect.commit()
        time.sleep(random.randint(1, 5))
        # i += 1
        # if i % 100 == 0:
        #     connect.commit()


def test_insert_data_6(self):
    connect = self.create_connect('datahub_zl')
    cursor = self.create_cursor(connect)
    # i = 1
    while True:
        sql = "insert into student_info(name,sex) VALUES(%s,%s)"
        val = (create_name(), random.choice(['F', 'M']))
        cursor.execute(sql, val)
        connect.commit()
        time.sleep(2)


def test_insert_data_7(self):
    connect = self.create_connect('datahub_zl')
    cursor = self.create_cursor(connect)
    for x in range(5):
        sql = "insert into mysql_zl_all(afloat,adouble,adecimal,abit,adate,atime,ayear,adatetime,atimestamp) " \
              "VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        # sql = "insert into mysql_zl_all(afloat,adouble,adecimal,abit) " \
        #       "VALUES(%s,%s，%s,%s)"
        now = time.localtime()
        val = (round(random.uniform(0, 100000), random.randint(1, 5)),
               round(random.uniform(0, 1000000), random.randint(1, 8)),
               round(random.uniform(0, 1000000), random.randint(1, 5)), random.randint(0, 1),
               time.strftime("%Y-%m-%d", now), time.strftime("%H:%M:%S", now), 2019,
               time.strftime("%Y-%m-%d %H:%M:%S", now), time.strftime("%Y-%m-%d %H:%M:%S", now))
        cursor.execute(sql, val)
        connect.commit()
        time.sleep(random.randint(1, 5))


def test_insert_data_8(self):
    connect = self.create_connect('datahub_zl')
    cursor = self.create_cursor(connect)
    # for x in range(random.randint(1, 5)):
    #     sql = "insert into zl_key(name,sex) VALUES(%s,%s)"
    #     val = (create_name(), random.choice(['F', 'M']))
    #     cursor.execute(sql, val)
    #     connect.commit()
    #     # time.sleep(random.randint(1, 3))
    while True:
        sql = "insert into zl_key(name,sex) VALUES(%s,%s)"
        val = (create_name(), random.choice(['F', 'M']))
        cursor.execute(sql, val)
        connect.commit()
        time.sleep(random.randint(1, 3))


def test_insert_data_9(self):
    connect = self.create_connect('datahub_zl')
    cursor = self.create_cursor(connect)
    # for x in range(random.randint(1, 5)):
    #     sql = "insert into zl_nokey(id,name,sex) VALUES(%s,%s,%s)"
    #     val = (random.randint(-32768, 32767), create_name(), random.choice(['F', 'M']))
    #     cursor.execute(sql, val)
    #     connect.commit()
    #     # time.sleep(random.randint(1, 3))
    while True:
        sql = "insert into zl_nokey(id,name,sex) VALUES(%s,%s,%s)"
        val = (random.randint(-32768, 32767), create_name(), random.choice(['F', 'M']))
        cursor.execute(sql, val)
        connect.commit()
        time.sleep(random.randint(1, 3))


def test_1(self):
    connect = self.create_connect('datahub_test')
    cursor = self.create_cursor(connect)
    while True:
        sql = "insert into sj_test_007(name) VALUES(%s)"
        val = (create_name())
        cursor.execute(sql, val)
        connect.commit()
        time.sleep(random.randint(1, 3))

