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
        if atomic_database.exist and not ordinary_database.exist:
            atomic_database.rename(ordinary_database.name)

            self.__user_interactor.notify(f'Changing continued after fail in previous launch and can\'t restore ordinary database, but atomic was restored')

            return False
        elif atomic_database.exist and ordinary_database.exist:
            answer = self.__user_interactor.ask(f'Changing failed in previous launch. Do you want to continue changing? (y/n)')
            if answer == 'y':
                pass #TODO
            elif answer == 'n':
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
            assert ordinary_database.exist, 'ordinary database does not exist'
            assert ordinary_database.engine == 'Ordinary', f'database {ordinary_database.name} engine must be Ordinary'

            atomic_database.create(engine = 'Atomic')
            tables = ordinary_database.tables

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
