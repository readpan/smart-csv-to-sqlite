import sqlite3
import csv
import os


class csv_to_sqlite:

    def __init__(self, cav_path):
        files = os.walk(cav_path)

        self.csvReaders = {}
        for r, d, f in files:
            for name in f:
                # 转换成reader
                if name.__contains__(".csv"):
                    self.csvReaders[name.split('.')[0]] = csv.reader(open(os.path.join(r, name)))

    # 导出为sqlite
    def export_to_sqlite(self, output_path):
        if os.path.exists(output_path):  # 文件存在则删除
            os.remove(output_path)
        conn = sqlite3.connect(output_path)  # 创建数据库
        cursor = conn.cursor()
        for table_name, reader in self.csvReaders.items():
            self.__create_table_by_str_list(table_name, reader, cursor)

        conn.commit()
        cursor.close()
        conn.close()

    # 表头处理
    def __create_table_by_str_list(self, table_name, reader, sqlite_cursor):
        list_reader = list(reader)
        field = ""
        for i in range(len(list_reader[0])):
            field += list_reader[0][i]
            field += ' '
            field += list_reader[1][i]
            field += ' '
            field += list_reader[2][i]
            field += ','
        field = field[:-1]

        for i, row in enumerate(list_reader):
            if i == 0:  # 第一行表头
                # print(str.format('CREATE TABLE IF NOT EXISTS {0}({1})', table_name, fi))
                sqlite_cursor.execute(str.format('CREATE TABLE IF NOT EXISTS {0}({1})', table_name, field))
            elif i == 1 or i == 2:
                continue
            else:  # 数据行
                for ii, field in enumerate(row):
                    row[ii] = '\'' + field + '\''
                # print(str.format('INSERT INTO {0} VALUES ({1})', table_name, ','.join(row)))
                sqlite_cursor.execute(str.format('INSERT INTO {0} VALUES ({1})', table_name, ','.join(row)))


c = csv_to_sqlite("D:\Githubs\csvs-to-sqlite\csvs")
c.export_to_sqlite("./data.db")
