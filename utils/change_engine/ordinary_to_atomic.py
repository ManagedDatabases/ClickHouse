#!/bin/python3
from clickhouse_driver import Client
from copy import deepcopy
from enum import Enum
from interface import Database
import sys


class UserInteractor:
    def __init__(self, default_answer=None):
        self.__default_answer = default_answer

    def ask(self, text):
        answer = None
        if self.__default_answer is not None:
            answer = self.__default_answer
        else:
            answer = input(text)

        return answer

    def notify(self, text): #TODO logging
        print(text)


class Action(Enum):
    CONTINUE = 1
    REVERT = 2
    CONVERT = 3
    TERMINATE = 4


class DBOrdinaryToAtomicConverter:
    def __init__(self, user_interactor=UserInteractor()):
        self.__atomic_prefix = '__temp_ordinary_to_atomic__'
        self.__client = Client('localhost')
        self.__user_interactor = deepcopy(user_interactor)

    def __get_atomic_name(self, ordinary_name):
        return f'{self.__atomic_prefix}{ordinary_name}'

    def __get_needed_action(self, ordinary_database, temp_database):
        if temp_database.exists:
            answer = self.__user_interactor.ask(
                f'Changing for database {ordinary_database.name} failed in previous launch.\n'
                 'Do you want to continue changing? (y/n)\n'
            )

            if answer == 'y':
                return Action.CONTINUE
            elif answer == 'n':
                return Action.REVERT
            else:
                self.__user_interactor.notify(f'Wrong answer, skipped')
                return Action.TERMINATE
        else:
            return Action.CONVERT

    def convert(self, ordinary_name):
        ordinary_database = Database(ordinary_name, self.__client)
        temp_database = Database(self.__get_atomic_name(ordinary_name), self.__client)

        action = self.__get_needed_action(ordinary_database, temp_database)

        if action == Action.CONVERT or action == Action.CONTINUE:
            if not ordinary_database.exists:
                raise ValueError(f'ordinary database \'{ordinary_database.name}\' does not exists')

            if ordinary_database.engine != 'Ordinary':
                raise ValueError(f'database {ordinary_database.name} engine must be Ordinary')

            if not temp_database.exists:
                temp_database.create(engine = 'Atomic')

            temp_database.move_tables(ordinary_database.tables)
            ordinary_database.drop()
            temp_database.rename(ordinary_name)

            assert temp_database.engine == 'Atomic', 'something went wrong'
        elif action == Action.REVERT:
            if not ordinary_database.exists:
                ordinary_database.create()

            ordinary_database.move_tables(temp_database.tables)
            temp_database.drop()
        elif action == Action.TERMINATE:
            pass


def main():
    converter = DBOrdinaryToAtomicConverter()

    dbs = sys.argv[1:]
    for db in dbs:
        converter.convert(db)


if __name__ == '__main__':
    main()
