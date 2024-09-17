import clickhouse_connect

class clickhouse_manager:
    def __init__(self, host, database, username, password) -> None:
        self.client = clickhouse_connect.get_client(host=host, database=database, username=username, password=password)
        pass

    def query(self, query):
        result = self.client.query(query)
        return result.result_rows
    
    def query(self, query, parameters = None):
        result = self.client.query(query=query, parameters=parameters)
        return result.result_rows
    
    def command(self, command):
        return self.client.command(cmd=command)

    def command(self, command, parameters = None):
        return self.client.command(cmd= command, parameters=parameters)

    def insert(self, table_name, data, column_names):
        self.client.insert(table_name, data, column_names)

