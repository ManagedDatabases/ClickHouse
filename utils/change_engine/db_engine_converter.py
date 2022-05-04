#!/usr/bin/python3
from clickhouse_driver import Client
from interface import Database
import sys
from enum import Enum
from copy import deepcopy


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


class DBEngineConverter:
    def __init__(self, user_interactor=UserInteractor()):
        self.__temporary_prefix = None
        self.__client = Client('localhost')
        self.__user_interactor = deepcopy(user_interactor)

    def __get_temporary_name(self, database_name):
        assert self.__temporary_prefix is not None
        return f'{self.__temporary_prefix}{database_name}'

    def __get_needed_action(self, database, temp_database):
        if temp_database.exists:

            intersect_tables = set([table.name for table in database.tables]) & set([table.name for table in temp_database.tables])
            tables = dict([(table.name, table) for table in database.tables])
            temp_tables = dict([(table.name, table) for table in temp_database.tables])

            for table in intersect_tables:
                if tables[table].row_count > temp_tables[table].row_count:
                    temp_tables[table].drop()
                else:
                    tables[table].drop()

            answer = self.__user_interactor.ask(
                f'Changing for database {database.name} failed in previous launch.\n'
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

    def convert(self, database_name, engine_from, engine_to, safe_rename=True): #TODO engine name validation
        self.__temporary_prefix = f'__temporary_{engine_from}_to_{engine_to}__'

        database = Database(database_name, self.__client)
        temp_database = Database(self.__get_temporary_name(database_name), self.__client)

        action = self.__get_needed_action(database, temp_database)

        if action == Action.CONVERT or action == Action.CONTINUE:
            if not database.exists:
                raise ValueError(f'database \'{database.name}\' does not exists')

            if database.engine != engine_from:
                raise ValueError(f'database {database.name} engine must be {engine_from}')

            if not temp_database.exists:
                temp_database.create(engine=engine_to)

            temp_database.move_tables(database.tables, safe_rename)
            database.drop()

            if engine_to == 'Atomic':
                temp_database.rename(database_name)
            else:
                database.create(engine=engine_to)
                database.move_tables(temp_database.tables, safe_rename)
                temp_database.drop()

            assert database.engine == engine_to, 'something went wrong'
        elif action == Action.REVERT:
            if not database.exists:
                database.create(engine=engine_from)

            if database.engine == engine_from:
                database.move_tables(temp_database.tables, safe_rename)
                temp_database.drop()
            else:
                temp_database.move_tables(database.tables, safe_rename)
                database.drop()
                database.create(engine=engine_from)
                database.move_tables(temp_database.tables, safe_rename)
                temp_database.drop()
        elif action == Action.TERMINATE:
            pass


def main():
    converter = DBEngineConverter()

    db, engine_from, engine_to = sys.argv[1:4]
    converter.convert(db, engine_from, engine_to)


if __name__ == '__main__':
    main()
