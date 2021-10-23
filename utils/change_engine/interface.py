class Table:
    def __init__(self, name, client):
        self.__name = name
        self.__client = client

    @property
    def name(self):
        return self.__name.split('.')[1]

    @property
    def database_name(self):
        return self.__name.split('.')[0]

    @property
    def fullname(self):
        return self.__name

    @property
    def exist(self):
        tables = self.__client.execute(f'SELECT name FROM system.tables WHERE database == {self.database_name} and name == \'{self.name}\'')
        return len(tables) == 1

    def create(self):
        assert not self.exist, f'Table {self.fullname} already exist'

        self.__client.execute(f'CREATE TABLE {self.fullname}')

    def rename(self, new_name):
        if self.exist:
            self.__client.execute(f'RENAME TABLE {self.name} TO {new_name}')

        self.__name = new_name

    def drop(self):
        assert self.exist, f'Table {self.name} does not exist'

        self.__client.execute(f'DROP TABLE {self.name}')


class Database:
    def __init__(self, name, client):
        self.__name = name
        self.__client = client

    @property
    def name(self):
        return self.__name

    @property
    def exist(self):
        dbs = self.__client.execute(f'SELECT name FROM system.databases WHERE name == \'{self.name}\'')
        return len(dbs) == 1

    @property
    def engine(self):
        assert self.exist, f'Database {self.name} does not exist'

        dbs = self.__client.execute(f'SELECT engine FROM system.databases WHERE name == \'{self.name}\'')
        return dbs[0][0]

    @property
    def tables(self):
        assert self.exist, f'Database {self.name} does not exist'

        def construct_table(table_name):
            return Table(f'{self.name}.{table_name}', self.__client)

        tables = self.__client.execute(f'SELECT name FROM system.tables WHERE database == \'{self.name}\'')
        return {name: construct_table(name) for name in tables}

    def get_table(self, table_name):
        assert self.exist, f'Database {self.name} does not exist'

        tables = self.__client.execute(f'SELECT name FROM system.tables WHERE database == \'{self.name}\' and name == \'{table_name}\'')
        assert len(tables) == 1, f'Table {table_name} does not exist'

        return Table(f'{self.name}.{table_name}', self.__client)

    def create(self, engine = 'Ordinary'):
        assert not self.exist, f'Database {self.name} already exist'

        self.__client.execute(f'CREATE DATABASE {self.name} ENGINE={engine}')

    def rename(self, new_name):
        if self.exist:
            self.__client.execute(f'RENAME DATABASE {self.name} TO {new_name}')

        self.__name = new_name

    def create_table(self, table_name):
        assert self.exist, f'Database {self.name} does not exist'

        table = Table(f'{self.name}.{table_name}', self.__client)
        table.create()

        return table

    def drop(self):
        assert self.exist, f'Database {self.name} does not exist'

        self.__client.execute(f'DROP DATABASE {self.name}')
