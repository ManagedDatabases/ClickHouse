from db_engine_converter import DBEngineConverter
from test import check_changing


# Cell towers


@check_changing(tables = 100, engine_from = 'Atomic', engine_to = 'Ordinary', tables_type = 'cell_towers')
def test_cell_towers_100(database_name):
    converter = DBEngineConverter()

    converter.convert(database_name, 'Atomic', 'Ordinary')


@check_changing(tables = 50, engine_from = 'Atomic', engine_to = 'Ordinary', tables_type = 'cell_towers')
def test_cell_towers_50(database_name):
    converter = DBEngineConverter()

    converter.convert(database_name, 'Atomic', 'Ordinary')


@check_changing(tables = 20, engine_from = 'Atomic', engine_to = 'Ordinary', tables_type = 'cell_towers')
def test_cell_towers_20(database_name):
    converter = DBEngineConverter()

    converter.convert(database_name, 'Atomic', 'Ordinary')


@check_changing(tables = 10, engine_from = 'Atomic', engine_to = 'Ordinary', tables_type = 'cell_towers')
def test_cell_towers_10(database_name):
    converter = DBEngineConverter()

    converter.convert(database_name, 'Atomic', 'Ordinary')


@check_changing(tables = 5, engine_from = 'Atomic', engine_to = 'Ordinary', tables_type = 'cell_towers')
def test_cell_towers_5(database_name):
    converter = DBEngineConverter()

    converter.convert(database_name, 'Atomic', 'Ordinary')


@check_changing(tables = 2, engine_from = 'Atomic', engine_to = 'Ordinary', tables_type = 'cell_towers')
def test_cell_towers_2(database_name):
    converter = DBEngineConverter()

    converter.convert(database_name, 'Atomic', 'Ordinary')


@check_changing(tables = 1, engine_from = 'Atomic', engine_to = 'Ordinary', tables_type = 'cell_towers')
def test_cell_towers_1(database_name):
    converter = DBEngineConverter()

    converter.convert(database_name, 'Atomic', 'Ordinary')


@check_changing(tables = 100, engine_from = 'Atomic', engine_to = 'Ordinary', tables_type = 'cell_towers')
def test_cell_towers_100_unsafe(database_name):
    converter = DBEngineConverter()

    converter.convert(database_name, 'Atomic', 'Ordinary', safe_rename=False)


@check_changing(tables = 50, engine_from = 'Atomic', engine_to = 'Ordinary', tables_type = 'cell_towers')
def test_cell_towers_50_unsafe(database_name):
    converter = DBEngineConverter()

    converter.convert(database_name, 'Atomic', 'Ordinary', safe_rename=False)


@check_changing(tables = 20, engine_from = 'Atomic', engine_to = 'Ordinary', tables_type = 'cell_towers')
def test_cell_towers_20_unsafe(database_name):
    converter = DBEngineConverter()

    converter.convert(database_name, 'Atomic', 'Ordinary', safe_rename=False)


@check_changing(tables = 10, engine_from = 'Atomic', engine_to = 'Ordinary', tables_type = 'cell_towers')
def test_cell_towers_10_unsafe(database_name):
    converter = DBEngineConverter()

    converter.convert(database_name, 'Atomic', 'Ordinary', safe_rename=False)


@check_changing(tables = 5, engine_from = 'Atomic', engine_to = 'Ordinary', tables_type = 'cell_towers')
def test_cell_towers_5_unsafe(database_name):
    converter = DBEngineConverter()

    converter.convert(database_name, 'Atomic', 'Ordinary', safe_rename=False)


@check_changing(tables = 2, engine_from = 'Atomic', engine_to = 'Ordinary', tables_type = 'cell_towers')
def test_cell_towers_2_unsafe(database_name):
    converter = DBEngineConverter()

    converter.convert(database_name, 'Atomic', 'Ordinary', safe_rename=False)


@check_changing(tables = 1, engine_from = 'Atomic', engine_to = 'Ordinary', tables_type = 'cell_towers')
def test_cell_towers_1_unsafe(database_name):
    converter = DBEngineConverter()

    converter.convert(database_name, 'Atomic', 'Ordinary', safe_rename=False)


# Numbers


@check_changing(tables = 100, engine_from = 'Atomic', engine_to = 'Ordinary')
def test_numbers_100(database_name):
    converter = DBEngineConverter()

    converter.convert(database_name, 'Atomic', 'Ordinary')


@check_changing(tables = 50, engine_from = 'Atomic', engine_to = 'Ordinary')
def test_numbers_50(database_name):
    converter = DBEngineConverter()

    converter.convert(database_name, 'Atomic', 'Ordinary')


@check_changing(tables = 20, engine_from = 'Atomic', engine_to = 'Ordinary')
def test_numbers_20(database_name):
    converter = DBEngineConverter()

    converter.convert(database_name, 'Atomic', 'Ordinary')


@check_changing(tables = 10, engine_from = 'Atomic', engine_to = 'Ordinary')
def test_numbers_10(database_name):
    converter = DBEngineConverter()

    converter.convert(database_name, 'Atomic', 'Ordinary')


@check_changing(tables = 5, engine_from = 'Atomic', engine_to = 'Ordinary')
def test_numbers_5(database_name):
    converter = DBEngineConverter()

    converter.convert(database_name, 'Atomic', 'Ordinary')


@check_changing(tables = 2, engine_from = 'Atomic', engine_to = 'Ordinary')
def test_numbers_2(database_name):
    converter = DBEngineConverter()

    converter.convert(database_name, 'Atomic', 'Ordinary')


@check_changing(tables = 1, engine_from = 'Atomic', engine_to = 'Ordinary')
def test_numbers_1(database_name):
    converter = DBEngineConverter()

    converter.convert(database_name, 'Atomic', 'Ordinary')


@check_changing(tables = 100, engine_from = 'Atomic', engine_to = 'Ordinary')
def test_numbers_100_unsafe(database_name):
    converter = DBEngineConverter()

    converter.convert(database_name, 'Atomic', 'Ordinary', safe_rename=False)


@check_changing(tables = 50, engine_from = 'Atomic', engine_to = 'Ordinary')
def test_numbers_50_unsafe(database_name):
    converter = DBEngineConverter()

    converter.convert(database_name, 'Atomic', 'Ordinary', safe_rename=False)


@check_changing(tables = 20, engine_from = 'Atomic', engine_to = 'Ordinary')
def test_numbers_20_unsafe(database_name):
    converter = DBEngineConverter()

    converter.convert(database_name, 'Atomic', 'Ordinary', safe_rename=False)


@check_changing(tables = 10, engine_from = 'Atomic', engine_to = 'Ordinary')
def test_numbers_10_unsafe(database_name):
    converter = DBEngineConverter()

    converter.convert(database_name, 'Atomic', 'Ordinary', safe_rename=False)


@check_changing(tables = 5, engine_from = 'Atomic', engine_to = 'Ordinary')
def test_numbers_5_unsafe(database_name):
    converter = DBEngineConverter()

    converter.convert(database_name, 'Atomic', 'Ordinary', safe_rename=False)


@check_changing(tables = 2, engine_from = 'Atomic', engine_to = 'Ordinary')
def test_numbers_2_unsafe(database_name):
    converter = DBEngineConverter()

    converter.convert(database_name, 'Atomic', 'Ordinary', safe_rename=False)


@check_changing(tables = 1, engine_from = 'Atomic', engine_to = 'Ordinary')
def test_numbers_1_unsafe(database_name):
    converter = DBEngineConverter()

    converter.convert(database_name, 'Atomic', 'Ordinary', safe_rename=False)
