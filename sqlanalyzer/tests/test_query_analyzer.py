import pytest
from sqlanalyzer import query_analyzer, unbundle


@pytest.fixture
def sample_query():
    query = """SELECT *
    FROM
        (SELECT a.*,
                b.*,
                c.*,
                d.*
        FROM
            (SELECT DISTINCT anonymous_id,
                            user_id
            FROM mapbox_customer_data.segment_identifies
            WHERE dt >= '2018-07-01'
            AND anonymous_id IS NOT NULL
            AND user_id IS NOT NULL ) a
        LEFT JOIN
            (SELECT id,
                    email,
                    created
            FROM mapbox_customer_data.accounts
            WHERE cast(dt AS DATE) = CURRENT_DATE - INTERVAL '1' DAY ) b ON a.user_id = b.id
        LEFT JOIN
            (SELECT anonymous_id AS anon_id_ad,
                    context_campaign_name,
                    min(TIMESTAMP) AS min_exposure
            FROM mapbox_customer_data.segment_pages
            WHERE dt >= '2018-07-01'
            AND context_campaign_name IS NOT NULL
            GROUP BY 1,
                    2) c ON a.anonymous_id = c.anon_id_ad
        LEFT JOIN
            (SELECT DISTINCT anonymous_id AS anon_id_event,
                            original_timestamp,
                            event,
                            context_traits_email
            FROM mapbox_customer_data.segment_tracks
            WHERE dt >= '2018-07-01'
            AND event LIKE 'submitted_%form'
            AND context_traits_email IS NOT NULL ) d ON a.anonymous_id = d.anon_id_event
        LEFT JOIN
            (SELECT sfdc_accounts.platform, sfdc_accounts.mobile_os, sfdc_accounts.service_metadata,
    sfdc_cases.account, sfdc_cases.num_requests, sfdc_cases.owner, sfdc_accounts.user_id
    FROM sfdc.accounts sfdc_accounts
    LEFT JOIN 
    (SELECT MAX(dt) FROM 
        (SELECT dt 
        FROM sfdc.oppty 
        LEFT JOIN (SELECT MAX(dt) FROM (SELECT DISTINCT dt FROM sfdc.owner AS sfdc_owner) AS dt_owner ON sfdc_oppty.dt = sfdc_cases.dt)
        LEFT JOIN (SELECT dt FROM sfdc.cases) sfdc_cases ON sfdc_oppty.dt = sfdc_cases.dt) )
    AS sfdc_cases_oppty ON sfdc_cases_oppty.dt = sfdc_accounts.dt
    LEFT JOIN sfdc.cases AS sfdc_cases ON sfdc_cases.id = sfdc_accounts.case_id
    WHERE sfdc_cases_oppty.dt > '2020-04-03' AND sfdc_cases_oppty.dt < '2020-05-04' ORDER BY 1 GROUP BY 3 LIMIT 20
            ) e ON e.user_id = a.user_id
            )
    WHERE context_campaign_name IS NOT NULL 
    """
    return query


@pytest.fixture
def analyzer(sample_query):
    analyzer = query_analyzer.Analyzer(sample_query)
    return analyzer


@pytest.fixture
def unbundled(sample_query):
    unbundled = unbundle.Unbundle(sample_query)
    return unbundled


@pytest.fixture
def cte_dict():
    return {'appuserturnstile_30d': "SELECT CASE\n              WHEN LOWER(useragent) RLIKE '(mapboxeventsnavigation|navigation-)' THEN 'navigation'\n              WHEN LOWER(useragent) RLIKE 'mapboxvision' THEN 'vision'\n              WHEN LOWER(useragent) RLIKE 'mapboxeventsunity' THEN 'unity'\n              WHEN LOWER(useragent) RLIKE 'mapboxtelemetry' THEN 'telemetry'\n              WHEN LOWER(useragent) RLIKE '(mapboxevents|mapbox-maps-)' THEN 'maps'\n              ELSE 'other'\n          END AS service,\n          COALESCE(OWNER, 'unknown') AS account,\n          userid\n   FROM sdk_events.china_appuserturnstile\n   WHERE dt BETWEEN DATE_SUB('{run_date}', 29) AND '{run_date}' ",
            'appuserturnstile_7d': "SELECT CASE\n              WHEN LOWER(useragent) RLIKE '(mapboxeventsnavigation|navigation-)' THEN 'navigation'\n              WHEN LOWER(useragent) RLIKE 'mapboxvision' THEN 'vision'\n              WHEN LOWER(useragent) RLIKE 'mapboxeventsunity' THEN 'unity'\n              WHEN LOWER(useragent) RLIKE 'mapboxtelemetry' THEN 'telemetry'\n              WHEN LOWER(useragent) RLIKE '(mapboxevents|mapbox-maps-)' THEN 'maps'\n              ELSE 'other'\n          END AS service,\n          COALESCE(OWNER, 'unknown') AS account,\n          userid\n   FROM sdk_events.china_appuserturnstile\n   WHERE dt BETWEEN DATE_SUB('{run_date}', 6) AND '{run_date}' ",
            'appuserturnstile_1d': "SELECT CASE\n              WHEN LOWER(useragent) RLIKE '(mapboxeventsnavigation|navigation-)' THEN 'navigation'\n              WHEN LOWER(useragent) RLIKE 'mapboxvision' THEN 'vision'\n              WHEN LOWER(useragent) RLIKE 'mapboxeventsunity' THEN 'unity'\n              WHEN LOWER(useragent) RLIKE 'mapboxtelemetry' THEN 'telemetry'\n              WHEN LOWER(useragent) RLIKE '(mapboxevents|mapbox-maps-)' THEN 'maps'\n              ELSE 'other'\n          END AS service,\n          COALESCE(OWNER, 'unknown') AS account,\n          userid\n   FROM sdk_events.china_appuserturnstile\n   WHERE dt = '{run_date}' ",
            'mau_ts_cube': "SELECT '30d' AS aggregation,\n          'mobile' AS platform,\n          COALESCE(service, '_all') AS service,\n          COALESCE(account, '_all') AS account,\n          num_users\n   FROM\n     (SELECT service,\n             account,\n             COUNT(DISTINCT userid) AS num_users\n      FROM appuserturnstile_30d\n      GROUP BY service,\n               account WITH CUBE) m",
            'wau_ts_cube': "SELECT '7d' AS aggregation,\n          'mobile' AS platform,\n          COALESCE(service, '_all') AS service,\n          COALESCE(account, '_all') AS account,\n          num_users\n   FROM\n     (SELECT service,\n             account,\n             COUNT(DISTINCT userid) AS num_users\n      FROM appuserturnstile_7d\n      GROUP BY service,\n               account WITH CUBE) w",
            'dau_ts_cube': "SELECT '1d' AS aggregation,\n          'mobile' AS platform,\n          COALESCE(service, '_all') AS service,\n          COALESCE(account, '_all') AS account,\n          num_users\n   FROM\n     (SELECT service,\n             account,\n             COUNT(DISTINCT userid) AS num_users\n      FROM appuserturnstile_1d\n      GROUP BY service,\n               account WITH CUBE) d)\nSELECT aggregation,\n       platform,\n       service,\n       account,\n       num_users\nFROM mau_ts_cube\nUNION ALL\nSELECT aggregation,\n       platform,\n       service,\n       account,\n       num_users\nFROM wau_ts_cube\nUNION ALL",
            'main_query': 'SELECT aggregation,\n       platform,\n       service,\n       account,\n       num_users\nFROM dau_ts_cube'}

def test_flatten_pure_nested(analyzer, sample_query):
    assert analyzer.flatten_pure_nested(sample_query) == [{'level_1_main': 'SELECT * WHERE context_campaign_name IS NOT NULL FROM no alias '},
                                                {'level_2_main': 'SELECT a.*,        b.*,        c.*,        d.* FROM a LEFT JOIN b ON a.user_id = b.id LEFT JOIN c ON a.anonymous_id = c.anon_id_ad LEFT JOIN d ON a.anonymous_id = d.anon_id_event LEFT JOIN e ON e.user_id = a.user_id '},
                                                {'a': "SELECT DISTINCT anonymous_id, user_id FROM mapbox_customer_data.segment_identifies WHERE dt >= '2018-07-01' AND anonymous_id IS NOT NULL AND user_id IS NOT NULL "},
                                                {'c': "SELECT anonymous_id AS anon_id_ad, context_campaign_name, min(TIMESTAMP) AS min_exposure FROM mapbox_customer_data.segment_pages WHERE dt >= '2018-07-01' AND context_campaign_name IS NOT NULL GROUP BY 1, 2"},
                                                {'b': "SELECT id, email, created FROM mapbox_customer_data.accounts WHERE cast(dt AS DATE) = CURRENT_DATE - INTERVAL '1' DAY "},
                                                {'e': "SELECT sfdc_accounts.platform,        sfdc_accounts.mobile_os,        sfdc_accounts.service_metadata,        sfdc_cases.account,        sfdc_cases.num_requests,        sfdc_cases.owner,        sfdc_accounts.user_id WHERE sfdc_cases_oppty.dt > '2020-04-03'   AND sfdc_cases_oppty.dt < '2020-05-04' FROM sfdc.accounts sfdc_accounts LEFT JOIN sfdc_cases_oppty ON sfdc_cases_oppty.dt = sfdc_accounts.dt LEFT JOIN sfdc.cases AS sfdc_cases ON sfdc_cases.id = sfdc_accounts.case_id "},
                                                {'sfdc_cases_oppty': 'SELECT MAX(dt) FROM ( FROM sfdc.oppty LEFT JOIN dt_owner ON sfdc_oppty.dt = sfdc_cases.dt) LEFT JOIN sfdc_cases ON sfdc_oppty.dt = sfdc_cases.dt '},
                                                {'sfdc_cases': 'SELECT dt FROM sfdc.cases'},
                                                {'dt_owner': 'SELECT MAX(dt) FROM ( FROM sfdc.owner AS sfdc_owner '}]


def test_flatten_cte_nested(analyzer, unbundled, cte_dict):
    assert analyzer.flatten_cte_nested(unbundled, cte_dict) == [{'appuserturnstile_30d': "SELECT CASE\n              WHEN LOWER(useragent) RLIKE '(mapboxeventsnavigation|navigation-)' THEN 'navigation'\n              WHEN LOWER(useragent) RLIKE 'mapboxvision' THEN 'vision'\n              WHEN LOWER(useragent) RLIKE 'mapboxeventsunity' THEN 'unity'\n              WHEN LOWER(useragent) RLIKE 'mapboxtelemetry' THEN 'telemetry'\n              WHEN LOWER(useragent) RLIKE '(mapboxevents|mapbox-maps-)' THEN 'maps'\n              ELSE 'other'\n          END AS service,\n          COALESCE(OWNER, 'unknown') AS account,\n          userid\n   FROM sdk_events.china_appuserturnstile\n   WHERE dt BETWEEN DATE_SUB('{run_date}', 29) AND '{run_date}' "},
                                                {'appuserturnstile_7d': "SELECT CASE\n              WHEN LOWER(useragent) RLIKE '(mapboxeventsnavigation|navigation-)' THEN 'navigation'\n              WHEN LOWER(useragent) RLIKE 'mapboxvision' THEN 'vision'\n              WHEN LOWER(useragent) RLIKE 'mapboxeventsunity' THEN 'unity'\n              WHEN LOWER(useragent) RLIKE 'mapboxtelemetry' THEN 'telemetry'\n              WHEN LOWER(useragent) RLIKE '(mapboxevents|mapbox-maps-)' THEN 'maps'\n              ELSE 'other'\n          END AS service,\n          COALESCE(OWNER, 'unknown') AS account,\n          userid\n   FROM sdk_events.china_appuserturnstile\n   WHERE dt BETWEEN DATE_SUB('{run_date}', 6) AND '{run_date}' "},
                                                {'appuserturnstile_1d': "SELECT CASE\n              WHEN LOWER(useragent) RLIKE '(mapboxeventsnavigation|navigation-)' THEN 'navigation'\n              WHEN LOWER(useragent) RLIKE 'mapboxvision' THEN 'vision'\n              WHEN LOWER(useragent) RLIKE 'mapboxeventsunity' THEN 'unity'\n              WHEN LOWER(useragent) RLIKE 'mapboxtelemetry' THEN 'telemetry'\n              WHEN LOWER(useragent) RLIKE '(mapboxevents|mapbox-maps-)' THEN 'maps'\n              ELSE 'other'\n          END AS service,\n          COALESCE(OWNER, 'unknown') AS account,\n          userid\n   FROM sdk_events.china_appuserturnstile\n   WHERE dt = '{run_date}' "},
                                                {'mau_ts_cube': [{'level_1_main': "SELECT '30d' AS aggregation,        'mobile' AS platform,        COALESCE(service, '_all') AS service,        COALESCE(account, '_all') AS account,        num_users FROM m "},
                                                {'m': 'SELECT service, account, COUNT(DISTINCT userid) AS num_users FROM appuserturnstile_30d GROUP BY service, account WITH CUBE'}]},
                                                {'wau_ts_cube': [{'level_1_main': "SELECT '7d' AS aggregation,        'mobile' AS platform,        COALESCE(service, '_all') AS service,        COALESCE(account, '_all') AS account,        num_users FROM w "},
                                                {'w': 'SELECT service, account, COUNT(DISTINCT userid) AS num_users FROM appuserturnstile_7d GROUP BY service, account WITH CUBE'}]},
                                                {'dau_ts_cube': [{'level_1_main': "SELECT '1d' AS aggregation,        'mobile' AS platform,        COALESCE(service, '_all') AS service,        COALESCE(account, '_all') AS account,        num_users FROM no alias FROM mau_ts_cube FROM wau_ts_cube "},
                                                {'no alias': 'SELECT service, account, COUNT(DISTINCT userid) AS num_users FROM appuserturnstile_1d GROUP BY service, account WITH CUBE) d'}]},
                                                {'main_query': 'SELECT aggregation,\n       platform,\n       service,\n       account,\n       num_users\nFROM dau_ts_cube'}]


def test_parse_query(analyzer, sample_query):             
    assert analyzer.parse_query(sample_query) == [{'level_1_main': 'SELECT * WHERE context_campaign_name IS NOT NULL FROM no alias '},
                                            {'level_2_main': 'SELECT a.*,        b.*,        c.*,        d.* FROM a LEFT JOIN b ON a.user_id = b.id LEFT JOIN c ON a.anonymous_id = c.anon_id_ad LEFT JOIN d ON a.anonymous_id = d.anon_id_event LEFT JOIN e ON e.user_id = a.user_id '},
                                            {'a': "SELECT DISTINCT anonymous_id, user_id FROM mapbox_customer_data.segment_identifies WHERE dt >= '2018-07-01' AND anonymous_id IS NOT NULL AND user_id IS NOT NULL "},
                                            {'c': "SELECT anonymous_id AS anon_id_ad, context_campaign_name, min(TIMESTAMP) AS min_exposure FROM mapbox_customer_data.segment_pages WHERE dt >= '2018-07-01' AND context_campaign_name IS NOT NULL GROUP BY 1, 2"},
                                            {'b': "SELECT id, email, created FROM mapbox_customer_data.accounts WHERE cast(dt AS DATE) = CURRENT_DATE - INTERVAL '1' DAY "},
                                            {'e': "SELECT sfdc_accounts.platform,        sfdc_accounts.mobile_os,        sfdc_accounts.service_metadata,        sfdc_cases.account,        sfdc_cases.num_requests,        sfdc_cases.owner,        sfdc_accounts.user_id WHERE sfdc_cases_oppty.dt > '2020-04-03'   AND sfdc_cases_oppty.dt < '2020-05-04' FROM sfdc.accounts sfdc_accounts LEFT JOIN sfdc_cases_oppty ON sfdc_cases_oppty.dt = sfdc_accounts.dt LEFT JOIN sfdc.cases AS sfdc_cases ON sfdc_cases.id = sfdc_accounts.case_id "},
                                            {'sfdc_cases_oppty': 'SELECT MAX(dt) FROM ( FROM sfdc.oppty LEFT JOIN dt_owner ON sfdc_oppty.dt = sfdc_cases.dt) LEFT JOIN sfdc_cases ON sfdc_oppty.dt = sfdc_cases.dt '},
                                            {'sfdc_cases': 'SELECT dt FROM sfdc.cases'},
                                            {'dt_owner': 'SELECT MAX(dt) FROM ( FROM sfdc.owner AS sfdc_owner '}]                       