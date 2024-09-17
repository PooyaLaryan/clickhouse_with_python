from ClickhouseManager import clickhouse_manager

def sample():
    ch_manager = clickhouse_manager('172.31.56.85','default','default','clickhouseP@ssword')
    
    ch_manager.command('DROP TABLE IF EXISTS python_sample_table')
    print("drop exists table")

    ch_manager.command('CREATE TABLE python_sample_table (key UInt32, name String, metric Float64) ENGINE MergeTree ORDER BY key')
    print("Create table successfuly")

    #batch insert
    row1 = [1000, 'String Value 1000', 5.233]
    row2 = [2000, 'String Value 2000', -107.04]
    data = [row1, row2]
    ch_manager.insert('python_sample_table', data, ['key', 'name', 'metric'])
    print("insert data successfuly")

    #insert one row 
    ch_manager.command("insert into python_sample_table values(3000,'String Value 3000', 3.15)")
    print("insert data successfuly")


    count = ch_manager.command("select count() from python_sample_table")
    print(f"count of rows {count}")

    result = ch_manager.query("Select * from python_sample_table")
    for i in result:
        print(f"{i[0]} --- {i[1]} --- {i[2]}")

    result = ch_manager.command("ALTER TABLE python_sample_table UPDATE metric=55.55 where key=3000;")
    print(result)
    print("Update data successfuly")

    result = ch_manager.query("Select * from python_sample_table")
    for i in result:
        print(f"{i[0]} --- {i[1]} --- {i[2]}")


if __name__ == '__main__':
    sample()