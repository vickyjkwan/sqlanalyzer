import pytest
from sqlanalyzer import column_parser


@pytest.fixture
def sample_query():
    query = """WITH opportunity_to_name AS 
                (SELECT  -- make sure there is only one name per id
                id AS account_id, name AS account_name FROM sfdc.accounts sfdc_accounts
                WHERE dt = '{run_date}' GROUP BY id, name) SELECT * FROM opportunity_to_name
    """
    return query


@pytest.fixture
def sample_query_diff_dbs():
    query = """
        SELECT * 
        from `some_database.schema.table`
        INNER JOIN some_schema.some_table
        WHERE column IS NULL
    """
    return query


@pytest.fixture
def formatter(sample_query):
    formatter = column_parser.Parser(sample_query)
    return formatter


def test_format_query(sample_query, formatter):
    assert formatter.format_query(sample_query) == "WITH opportunity_to_name AS\n  (SELECT id AS account_id,\n          name AS account_name\n   FROM sfdc.accounts sfdc_accounts\n   WHERE dt = '{run_date}'\n   GROUP BY id,\n            name)\nSELECT *\nFROM opportunity_to_name"


def test_parse_cte(sample_query, formatter):
    formatted_query = formatter.format_query(sample_query)
    cte_dict = formatter.parse_cte(formatted_query)

    assert cte_dict == {'opportunity_to_name': "SELECT id AS account_id,\n          name AS account_name\n   FROM sfdc.accounts sfdc_accounts\n   WHERE dt = '{run_date}'\n   GROUP BY id,\n            name",
                        'main_query': 'SELECT *\nFROM opportunity_to_name'}


def test_get_table_names(sample_query, formatter):
    formatted_query = formatter.format_query(sample_query)
    table_name_mapping = formatter.get_table_names(formatted_query.split('\n'))

    assert table_name_mapping == {'sfdc_accounts': 'sfdc.accounts',
                                    'opportunity_to_name': 'opportunity_to_name'}


def test_get_table_names_diff_dbs(sample_query_diff_dbs, formatter):
    formatted_query = formatter.format_query(sample_query_diff_dbs)
    table_name_mapping = formatter.get_table_names(formatted_query.split('\n'))

    assert table_name_mapping == {'`some_database.schema.table`': '`some_database.schema.table`',
                                    'some_schema.some_table': 'some_schema.some_table'}

  
