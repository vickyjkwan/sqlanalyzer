This is a Python package that parses a given sql query, matches the column and tables within your given metastore, and analyzes the query to generate a list of referenced columns within the metastore.


## Quick Start

`$ pip install sqlanalyzer`


## Example Usage

**1. Format a query to follow the [ANSI standards](https://blog.ansi.org/2018/10/sql-standard-iso-iec-9075-2016-ansi-x3-135/) for SQL:**


```
>>> from sqlanalyzer import column_parser
>>> query = """SELECT api.name, acct.customer_tier_c, acct.name FROM api_requests_by_account api
... LEFT JOIN accounts 
... acct ON api.user_id = acct.customer_api_id
... """
>>> formatter = column_parser.Parser(query)
>>> formatted = formatter.format_query(query)
>>> print(formatted)
SELECT api.name,
       acct.customer_tier_c,
       acct.name
FROM api_requests_by_account api
LEFT JOIN accounts acct ON api.user_id = acct.customer_api_id
```

**2. Separate CTE's and extract alias names and queries:**

```
>>> query = """WITH a AS
...   (SELECT DISTINCT anonymous_id,
...                    user_id
...    FROM customer_data.segment_identifies
...    WHERE dt >= '2018-07-01'),
...      b AS
...   (SELECT id,
...           email,
...           created
...    FROM customer_data.accounts)
... SELECT a.*,
...        b.*
... FROM a
... LEFT JOIN b ON a.user_id = b.id
... WHERE context_campaign_name IS NOT NULL
... """
>>> formatter = column_parser.Parser(query)
>>> cte_query = formatter.parse_cte(query)
>>> cte_query
{'a': "SELECT DISTINCT anonymous_id,\n                   user_id\n   FROM customer_data.segment_identifies\n   WHERE dt >= '2018-07-01'",
'b': 'SELECT id,\n          email,\n          created\n   FROM customer_data.accounts', 
'main_query': 'SELECT a.*,\n       b.*\nFROM a\nLEFT JOIN b ON a.user_id = b.id\nWHERE context_campaign_name IS NOT NULL\n'}
>>> cte_query.keys()
dict_keys(['a', 'b', 'main_query'])
```

**3. Match table aliases with the actual database name:**

```
>>> query = """SELECT *
... FROM api_requests.requests_by_account m
... INNER JOIN mapbox_customer_data.styles s ON m.metadata_version = s.id
... LEFT JOIN sfdc.users u ON m.csm = u.id
... """
>>> formatter = column_parser.Parser(query)
>>> formatted = formatter.format_query(query)
>>> table_alias_mapping = formatter.get_table_names(formatted.split('\n'))
>>> table_alias_mapping
{'m': 'api_requests.requests_by_account', 
's': 'mapbox_customer_data.styles', 
'u': 'sfdc.users'}
```


**4. Analyze and parse complex query with subqueries, Common Table Expressions and a mix of the two types.**

*a)* Parse multiple and deeply (3+ levels) nested subqueries:

```
>>> from sqlanalyzer import query_analyzer
>>> query = """SELECT *
... FROM
...   (SELECT a.*,
...           b.*
...    FROM
...      (SELECT DISTINCT anonymous_id,
...                       user_id
...       FROM customer_data.segment_identifies
...       WHERE dt >= '2018-07-01') a
...    LEFT JOIN
...      (SELECT id,
...              email,
...              created
...       FROM customer_data.accounts) b ON a.user_id = b.id
...    WHERE context_campaign_name IS NOT NULL )
... """
>>> analyzer = query_analyzer.Analyzer(query)
>>> analyzer.parse_query(query)
[{'level_1_main': 'SELECT * FROM no alias '}, 
{'level_2_main': 'SELECT a.*,        b.* WHERE context_campaign_name IS NOT NULL FROM a LEFT JOIN b ON a.user_id = b.id '}, 
{'a': "SELECT DISTINCT anonymous_id, user_id FROM customer_data.segment_identifies WHERE dt >= '2018-07-01'"}, 
{'b': 'SELECT id, email, created FROM customer_data.accounts'}]
```

*b)* Parse Common Table Expressions (CTE's):

```
>>> query = """WITH a AS
...   (SELECT DISTINCT anonymous_id,
...                    user_id
...    FROM customer_data.segment_identifies
...    WHERE dt >= '2018-07-01'),
...      b AS
...   (SELECT id,
...           email,
...           created
...    FROM customer_data.accounts)
... SELECT a.*,
...        b.*
... FROM a
... LEFT JOIN b ON a.user_id = b.id
... WHERE context_campaign_name IS NOT NULL
... """
>>> analyzer = query_analyzer.Analyzer(query)
>>> analyzer.parse_query(query)
[{'a': "SELECT DISTINCT anonymous_id,\n                   user_id\n   FROM customer_data.segment_identifies\n   WHERE dt >= '2018-07-01'"}, 
{'b': 'SELECT id,\n          email,\n          created\n   FROM customer_data.accounts'}, 
{'main_query': 'SELECT a.*,\n       b.*\nFROM a\nLEFT JOIN b ON a.user_id = b.id\nWHERE context_campaign_name IS NOT NULL'}]
```

*c)* Parse mixed type of nested queries and CTE's:

```
>>> query = """SELECT email,
...        COUNT(DISTINCT context_campaign_name)
... FROM
...   (WITH a AS
...      (SELECT DISTINCT anonymous_id,
...                       user_id
...       FROM customer_data.segment_identifies
...       WHERE dt >= '2018-07-01'),
...         b AS
...      (SELECT id,
...              email,
...              created
...       FROM customer_data.accounts) SELECT a.*,
...                                           b.*
...    FROM a
...    LEFT JOIN b ON a.user_id = b.id
...    WHERE context_campaign_name IS NOT NULL )
... WHERE user_id IN ('123',
...                   '234',
...                   '345')
... GROUP BY 1
... ORDER BY 2 DESC
... LIMIT 200
... """
>>> analyzer = query_analyzer.Analyzer(query)
>>> analyzer.parse_query(query)
[{'level_1_main': "SELECT email,        COUNT(DISTINCT context_campaign_name) WHERE user_id IN ('123',                   '234',                   '345') FROM no alias "}, 
{'no alias': [{'a': "SELECT DISTINCT anonymous_id,\n                   user_id\n   FROM customer_data.segment_identifies\n   WHERE dt >= '2018-07-01'"}, 
{'b': 'SELECT id,\n          email,\n          created\n   FROM customer_data.accounts'}, 
{'main_query': 'SELECT a.*,\n       b.*\nFROM a\nLEFT JOIN b ON a.user_id = b.id\nWHERE context_campaign_name IS NOT NULL'}]}]
```



Notes: 

[Upload instructions](https://packaging.python.org/tutorials/packaging-projects/)
`python3 -m pip install --user --upgrade setuptools wheel twine`
`python3 setup.py sdist bdist_wheel`
`twine check dist/*`
`twine upload dist/*`