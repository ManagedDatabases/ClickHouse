import pytest
from clickhouse_driver import Client
import random
import logging
from ordinary_to_atomic import DBOrdinaryToAtomicConverter
from interface import Database


client = Client('localhost')


def check_changing(tables, engine_from, engine_to):
    def wrapper(function, *args, **kwargs):
        def wrapped():
            name = f'test_{random.randint(1, 100000)}'

            client.execute(f'create database {name} engine={engine_from}')

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

            res = function(*args, database_name = name, **kwargs)

            correct_engine = database.engine == engine_to 
            correct_length = len(database.tables) == tables

            client.execute(f'drop database {name}')

            assert correct_engine and correct_length
            return res
        return wrapped
    return wrapper


@check_changing(tables = 10, engine_from = 'Ordinary', engine_to='Atomic')
def test_convert(database_name):
    converter = DBOrdinaryToAtomicConverter()

    converter.convert(database_name)


@check_changing(tables = 1000, engine_from = 'Ordinary', engine_to='Atomic')
def test_stress(database_name):
    converter = DBOrdinaryToAtomicConverter()

    converter.convert(database_name)
