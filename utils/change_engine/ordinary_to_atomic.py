#!/bin/python3
from clickhouse_driver import Client
from interface import Database
import sys


class UserInteractor:
    def ask(self, text):
        answer = input(text)
        return answer

    def notify(self, text): #TODO logging
        print(text)


class DBOrdinaryToAtomicConverter:
    def __init__(self):
        self.__atomic_prefix = '__temp_ordinary_to_atomic__'
        self.__client = Client('localhost')
        self.__user_interactor = UserInteractor()

    def __get_atomic_name(self, ordinary_name):
        return f'{self.__atomic_prefix}{ordinary_name}'

    def __finished_previous_sessions(self, ordinary_database, atomic_database):
        if atomic_database.exist:
            answer = self.__user_interactor.ask(
                f'Changing for database {ordinary_database.name} failed in previous launch.\n'
                 'Do you want to continue changing? (y/n)\n'
            )

            if answer == 'y':
                atomic_database.move_tables(ordinary_database.tables)
                ordinary_database.drop()
                atomic_database.rename(ordinary_database.name)
            elif answer == 'n':
                if not ordinary_database.exist:
                    ordinary_database.create()

                ordinary_database.move_tables(atomic_database.tables)
                atomic_database.drop()
            else:
                self.__user_interactor.notify(f'Wrong answer, skipped')

            return False
        else:
            return True

    def convert(self, ordinary_name):
        ordinary_database = Database(ordinary_name, self.__client)
        atomic_database = Database(self.__get_atomic_name(ordinary_name), self.__client)

        if self.__finished_previous_sessions(ordinary_database, atomic_database):
            if not ordinary_database.exist:
                raise ValueError(f'ordinary database \'{ordinary_database.name}\' does not exist')

            if ordinary_database.engine != 'Ordinary':
                raise ValueError(f'database {ordinary_database.name} engine must be Ordinary')

            atomic_database.create(engine = 'Atomic')
            tables = ordinary_database.tables

            for table in tables:
                table_name = table.name
                ordinary_table = ordinary_database.get_table(table_name)
                atomic_table_name = f'{atomic_database.name}.{table_name}'
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
