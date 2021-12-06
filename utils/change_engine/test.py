from clickhouse_driver import Client
from interface import Database
from ordinary_to_atomic import DBOrdinaryToAtomicConverter, UserInteractor
import logging
import multiprocessing
import pytest
import random
import time


client = Client('localhost')


def check_changing(tables, engine_from, engine_to):
    def wrapper(function, *args, **kwargs):
        def wrapped():
            name = f'test_{random.randint(1, 100000)}'

            client.execute(f'CREATE DATABASE {name} ENGINE = {engine_from}')

            database = Database(name, client)

            for i in range(tables):
                client.execute(
                    f'''
                        CREATE TABLE {name}.table_{i}(　
                            id UInt32,
                            name String
                        ) ENGINE = MergeTree ORDER BY (id, name);
                    '''
                    )

            table_names = sorted([t.name for t in database.tables])

            res = function(*args, database_name = name, **kwargs)

            correct_engine = database.engine == engine_to
            correct_length = len(database.tables) == tables
            correct_tables = table_names == sorted([t.name for t in database.tables])

            client.execute(f'drop database {name}')

            assert correct_engine
            assert correct_length
            assert correct_tables

            return res
        return wrapped
    return wrapper


@check_changing(tables = 10, engine_from = 'Ordinary', engine_to='Atomic')
def test_convert_ordinary_to_atomic(database_name):
    converter = DBOrdinaryToAtomicConverter()

    converter.convert(database_name)


@check_changing(tables = 1000, engine_from = 'Ordinary', engine_to='Atomic')
def test_stress_ordinary_to_atomic(database_name):
    converter = DBOrdinaryToAtomicConverter()

    converter.convert(database_name)


@check_changing(tables = 1000, engine_from = 'Ordinary', engine_to='Atomic')
def test_continue_after_fail_ordinary_to_atomic(database_name):
    converter = DBOrdinaryToAtomicConverter(UserInteractor('y'))

    process = multiprocessing.Process(target=converter.convert, args=(database_name,))
    process.start()

    time.sleep(1)

    process.terminate()

    converter.convert(database_name)


@check_changing(tables = 1000, engine_from = 'Ordinary', engine_to='Ordinary')
def test_revert_after_fail_ordinary_to_atomic(database_name):
    converter = DBOrdinaryToAtomicConverter(UserInteractor('n'))

    process = multiprocessing.Process(target=converter.convert, args=(database_name,))
    process.start()

    time.sleep(1)

    process.terminate()

    converter.convert(database_name)
