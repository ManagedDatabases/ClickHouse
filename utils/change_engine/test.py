from clickhouse_driver import Client
from interface import Database
from db_engine_converter import DBEngineConverter, UserInteractor
import multiprocessing
import pytest
import random
import time


client = Client('localhost')


def check_changing(tables, engine_from, engine_to, tables_type='numbers'):
    def wrapper(function, *args, **kwargs):
        def wrapped():
            name = f'test_{random.randint(1, 100000)}'

            client.execute(f'CREATE DATABASE {name} ENGINE = {engine_from};')

            database = Database(name, client)

            row_counts = {}

            for i in range(tables):
                if tables_type == 'numbers':
                    client.execute(
                        f'''
                            CREATE TABLE {name}.table_{i}(　
                                id UInt32
                            ) ENGINE = MergeTree ORDER BY id;
                        '''
                        )

                    row_count = random.randint(1, 100)

                    values = ('({})\n' * row_count).format(*range(row_count))
                    client.execute(f'INSERT INTO {name}.table_{i} VALUES {values};')

                    row_counts[f'{name}.table_{i}'] = row_count
                elif tables_type == 'cell_towers':
                    client.execute(
                        f'''
                            CREATE TABLE {name}.table_{i} AS cell_towers
                        '''
                        )

                    client.execute(
                        f'''
                            INSERT INTO {name}.table_{i} SELECT * FROM cell_towers
                        '''
                        )

                    row_count = client.execute(f'SELECT COUNT(*) FROM {name}.table_{i}')[0][0]
                    row_counts[f'{name}.table_{i}'] = row_count

            table_names = sorted([t.name for t in database.tables])

            begin = time.time()

            res = function(*args, database_name = name, **kwargs)

            elapsed_time = (time.time() - begin)
            print(f'elapsed time: {elapsed_time}')

            correct_engine = database.engine == engine_to

            if not correct_engine:
                print('wrong engine', database.engine, engine_to)

            correct_length = len(database.tables) == tables

            if not correct_length:
                print('wrong count', len(database.tables), tables)

            correct_tables = table_names == sorted([t.name for t in database.tables])
            correct_data = True

            for table_name, row_count in row_counts.items():
                count = client.execute(f'SELECT COUNT(*) FROM {table_name};')[0][0]
                correct_data &= count == row_count

            client.execute(f'drop database {name};')

            assert correct_engine
            assert correct_length
            assert correct_tables
            assert correct_data

            return res
        return wrapped
    return wrapper


# Ordinary to Atomic


@check_changing(tables = 10, engine_from = 'Ordinary', engine_to = 'Atomic')
def test_convert_ordinary_to_atomic(database_name):
    converter = DBEngineConverter()

    converter.convert(database_name, 'Ordinary', 'Atomic')


@check_changing(tables = 1000, engine_from = 'Ordinary', engine_to = 'Atomic')
def test_stress_ordinary_to_atomic(database_name):
    converter = DBEngineConverter()

    converter.convert(database_name, 'Ordinary', 'Atomic')


@check_changing(tables = 500, engine_from = 'Ordinary', engine_to = 'Atomic')
def test_continue_after_fail_ordinary_to_atomic(database_name):
    converter = DBEngineConverter(UserInteractor('y'))

    process = multiprocessing.Process(target=converter.convert, args=(database_name, 'Ordinary', 'Atomic'))
    process.start()

    time.sleep(1)

    process.terminate()

    time.sleep(0.5)

    converter.convert(database_name, 'Ordinary', 'Atomic')


@check_changing(tables = 500, engine_from = 'Ordinary', engine_to = 'Ordinary')
def test_revert_after_fail_ordinary_to_atomic(database_name):
    converter = DBEngineConverter(UserInteractor('n'))

    process = multiprocessing.Process(target=converter.convert, args=(database_name, 'Ordinary', 'Atomic'))
    process.start()

    time.sleep(1)

    process.terminate()

    time.sleep(0.5)

    converter.convert(database_name, 'Ordinary', 'Atomic')


@check_changing(tables = 500, engine_from = 'Ordinary', engine_to = 'Atomic')
def test_atomicity_table_rename_ordinary_to_atomic(database_name):
    for i in range(1000):
        converter = DBEngineConverter(UserInteractor('n'))

        process = multiprocessing.Process(target=converter.convert, args=(database_name, 'Ordinary', 'Atomic', False))
        process.start()

        time.sleep(0.5)

        process.terminate()

        time.sleep(0.5)

    converter = DBEngineConverter(UserInteractor('y'))
    converter.convert(database_name, 'Ordinary', 'Atomic')


@check_changing(tables = 5000, engine_from = 'Ordinary', engine_to = 'Atomic')
def test_convert_fails_ordinary_to_atomic(database_name):
    for i in range(10):
        converter = DBEngineConverter(UserInteractor('y'))

        process = multiprocessing.Process(target=converter.convert, args=(database_name, 'Ordinary', 'Atomic'))
        process.start()

        time.sleep(1)

        process.terminate()

        time.sleep(0.5)

    converter = DBEngineConverter(UserInteractor('y'))
    converter.convert(database_name, 'Ordinary', 'Atomic')

# Atomic to Ordinary


@check_changing(tables = 10, engine_from = 'Atomic', engine_to = 'Ordinary')
def test_convert_atomic_to_ordinary(database_name):
    converter = DBEngineConverter()

    converter.convert(database_name, 'Atomic', 'Ordinary')


@check_changing(tables = 1000, engine_from = 'Atomic', engine_to = 'Ordinary')
def test_stress_atomic_to_ordinary(database_name):
    converter = DBEngineConverter()

    converter.convert(database_name, 'Atomic', 'Ordinary')


@check_changing(tables = 500, engine_from = 'Atomic', engine_to = 'Ordinary')
def test_continue_after_fail_atomic_to_ordinary(database_name):
    converter = DBEngineConverter(UserInteractor('y'))

    process = multiprocessing.Process(target=converter.convert, args=(database_name, 'Atomic', 'Ordinary'))
    process.start()

    time.sleep(1)

    process.terminate()

    time.sleep(0.5)

    converter.convert(database_name, 'Atomic', 'Ordinary')


@check_changing(tables = 500, engine_from = 'Atomic', engine_to = 'Atomic')
def test_revert_after_fail_atomic_to_ordinary(database_name):
    converter = DBEngineConverter(UserInteractor('n'))

    process = multiprocessing.Process(target=converter.convert, args=(database_name, 'Atomic', 'Ordinary'))
    process.start()

    time.sleep(1)

    process.terminate()

    time.sleep(0.5)

    converter.convert(database_name, 'Atomic', 'Ordinary')


@check_changing(tables = 500, engine_from = 'Atomic', engine_to = 'Ordinary')
def test_atomicity_table_rename_atomic_to_ordinary(database_name):
    for i in range(1000):
        converter = DBEngineConverter(UserInteractor('n'))

        process = multiprocessing.Process(target=converter.convert, args=(database_name, 'Atomic', 'Ordinary', False))
        process.start()

        time.sleep(0.5)

        process.terminate()

        time.sleep(0.5)

    converter = DBEngineConverter(UserInteractor('y'))
    converter.convert(database_name, 'Atomic', 'Ordinary')


@check_changing(tables = 500, engine_from = 'Atomic', engine_to = 'Ordinary')
def test_convert_fails_atomic_to_ordinary(database_name):
    for i in range(500):
        converter = DBEngineConverter(UserInteractor('y'))

        process = multiprocessing.Process(target=converter.convert, args=(database_name, 'Atomic', 'Ordinary'))
        process.start()

        time.sleep(1)

        process.terminate()

        time.sleep(0.5)

    converter = DBEngineConverter(UserInteractor('y'))
    converter.convert(database_name, 'Atomic', 'Ordinary')
