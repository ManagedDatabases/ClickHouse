#!/bin/python3
from clickhouse_driver import Client
from interface import Database
import sys


class DBOrdinaryToAtomicConverter:
    def __init__(self):
        self.__atomic_prefix = '__temp_ordinary_to_atomic__'
        self.__client = Client('localhost')

    def __continue_change(self, atomic_name):
        self.__rename_atomic_db(atomic_name)

    def __abort_change(self, atomic_name):
        self.__drop_database(atomic_name)

    def __get_atomic_name(self, ordinary_name):
        return f'{self.__atomic_prefix}{ordinary_name}'

    def __remove_atomic_prefix(self, name):
        assert atomic_name.startswith(self.__atomic_prefix)

        prefix_len = len(self.__atomic_prefix)
        return name[prefix_len:]

    def convert(self, ordinary_name):
        ordinary_database = Database(ordinary_name, self.__client)

        assert ordinary_database.exist, 'ordinary database does not exist'
        assert ordinary_database.engine == 'Ordinary', 'something went wrong'

        tables = ordinary_database.tables
        atomic_database = Database(self.__get_atomic_name(ordinary_name), self.__client)

        for table in tables:
            ordinary_table = ordinary_database.get_table(table)
            atomic_table_name = f'{atomic_database.name}.{table}'
            ordinary_table.rename(atomic_table_name)

        ordinary_database.drop()
        atomic_database.rename(ordinary_name)

        assert atomic_database.engine == 'Atomic', 'something went wrong'


def main():
    converter = DBOrdinaryToAtomicConverter()

    dbs = sys.argv[1:]
    for db in dbs:
        converter.convert(db)


if __name__ == '__main__':
    main()
