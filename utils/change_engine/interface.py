class Table:
    def __init__(self, name, client):
        self.__fullname = name
        self.__database, self.__name = name.split('.')
        self.__client = client

    @property
    def name(self):
        return self.__name

    @property
    def database_name(self):
        return self.__database

    @property
    def fullname(self):
        return self.__fullname

    @property
    def exists(self):
        tables = self.__client.execute(f'SELECT name FROM system.tables WHERE database == \'{self.database_name}\' and name == \'{self.name}\'')
        return len(tables) == 1

    def create(self):
        assert not self.exists, f'Table {self.fullname} already exists'

        self.__client.execute(f'CREATE TABLE {self.fullname}')

    def rename_table(self, new_name):
        assert self.exists, f'Table {self.fullname} does not exists'

        self.__client.execute(f'RENAME TABLE {self.fullname} TO {new_name}')
        self.__fullname = new_name
        self.__name, self.__database = self.__fullname.split('.')

    def rename_database(self, new_database):
        self.rename_table(f'{new_database}.{self.name}')

    def drop(self):
        assert self.exists, f'Table {self.fullname} does not exists'

        self.__client.execute(f'DROP TABLE {self.fullname}')


class Database:
    def __init__(self, name, client):
        self.__name = name
        self.__client = client

    @property
    def name(self):
        return self.__name

    @property
    def exists(self):
        dbs = self.__client.execute(f'SELECT name FROM system.databases WHERE name == \'{self.name}\'')
        return len(dbs) == 1

    @property
    def engine(self):
        assert self.exists, f'Database {self.name} does not exists'

        dbs = self.__client.execute(f'SELECT engine FROM system.databases WHERE name == \'{self.name}\'')
        return dbs[0][0]

    @property
    def tables(self):
        assert self.exists, f'Database {self.name} does not exists'

        tables = self.__client.execute(f'SELECT name FROM system.tables WHERE database == \'{self.name}\'')
        tables = map(lambda row: row[0], tables)

        return [self.__construct_table(name) for name in tables]

    def __construct_table(self, table_name):
        return Table(f'{self.name}.{table_name}', self.__client)

    def get_table(self, table_name):
        assert self.exists, f'Database {self.name} does not exists'

        tables = self.__client.execute(f'SELECT name FROM system.tables WHERE database == \'{self.name}\' and name == \'{table_name}\'')
        assert len(tables) == 1, f'Table {table_name} does not exists'

        return self.__construct_table(table_name)

    def create(self, engine = 'Ordinary'):
        assert not self.exists, f'Database {self.name} already exists'

        self.__client.execute(f'CREATE DATABASE {self.name} ENGINE={engine}')

    def rename(self, new_name):
        assert self.exists, f'Database {self.name} does not exists'

        self.__client.execute(f'RENAME DATABASE {self.name} TO {new_name}')
        self.__name = new_name

    def create_table(self, table_name):
        assert self.exists, f'Database {self.name} does not exists'

        table = self.__construct_table(table_name)
        table.create()

        return table

    def move_tables(self, tables):
        for table in tables:
            table.rename_database(self.name)

    def drop(self):
        assert self.exists, f'Database {self.name} does not exists'

        self.__client.execute(f'DROP DATABASE {self.name}')
