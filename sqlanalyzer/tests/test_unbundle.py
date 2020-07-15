import pytest
from sqlanalyzer import column_parser, unbundle

@pytest.fixture
def sample_query():
    query = """SELECT *
            FROM
                (SELECT *
                FROM
                    (SELECT *
                    FROM
                        (SELECT a.* ,
                                b.* ,
                                c.* ,
                                d.*
                            FROM
                            (SELECT DISTINCT anonymous_id ,
                                            user_id
                            FROM mapbox_customer_data.segment_identifies
                            WHERE dt >= '2018-07-01'
                                AND anonymous_id IS NOT NULL
                                AND user_id IS NOT NULL ) a
                            LEFT JOIN
                            (SELECT id ,
                                    email ,
                                    created
                            FROM mapbox_customer_data.accounts
                            WHERE cast(dt AS DATE) = CURRENT_DATE - INTERVAL '1' DAY ) b ON a.user_id = b.id
                            LEFT JOIN
                            (SELECT anonymous_id AS anon_id_ad ,
                                    context_campaign_name ,
                                    min(TIMESTAMP) AS min_exposure
                            FROM mapbox_customer_data.segment_pages
                            WHERE dt >= '2018-07-01'
                                AND context_campaign_name IS NOT NULL
                            GROUP BY 1,
                                        2) c ON a.anonymous_id = c.anon_id_ad
                            LEFT JOIN
                            (SELECT DISTINCT anonymous_id AS anon_id_event ,
                                            original_timestamp ,
                                            event ,
                                            context_traits_email
                            FROM mapbox_customer_data.segment_tracks
                            WHERE dt >= '2018-07-01'
                                AND event LIKE 'submitted_%form'
                                AND context_traits_email IS NOT NULL ) d ON a.anonymous_id = d.anon_id_event)
                    WHERE context_campaign_name IS NOT NULL )
                LIMIT 0) T
            LIMIT 0
    """
    return query


@pytest.fixture
def formatter(sample_query):
    formatter = column_parser.Parser(sample_query)
    return formatter


@pytest.fixture
def unbundled(sample_query):
    unbundled = unbundle.Unbundle(sample_query)
    return unbundled


@pytest.fixture
def query_list(formatter, sample_query):
    formatted_query = formatter.format_query(sample_query)
    query_list = formatted_query.split('\n')
    return query_list


@pytest.fixture
def main_query():
    return ['SELECT *']


@pytest.fixture
def sub_query(copy_query_list):
    return ' '.join(copy_query_list)


@pytest.fixture
def copy_query_list():
    return ['FROM',
            '  (SELECT *',
            '   FROM',
            '     (SELECT *',
            '      FROM',
            '        (SELECT a.*,',
            '                b.*,',
            '                c.*,',
            '                d.*',
            '         FROM',
            '           (SELECT DISTINCT anonymous_id,',
            '                            user_id',
            '            FROM mapbox_customer_data.segment_identifies',
            "            WHERE dt >= '2018-07-01'",
            '              AND anonymous_id IS NOT NULL',
            '              AND user_id IS NOT NULL ) a',
            '         LEFT JOIN',
            '           (SELECT id,',
            '                   email,',
            '                   created',
            '            FROM mapbox_customer_data.accounts',
            "            WHERE cast(dt AS DATE) = CURRENT_DATE - INTERVAL '1' DAY ) b ON a.user_id = b.id",
            '         LEFT JOIN',
            '           (SELECT anonymous_id AS anon_id_ad,',
            '                   context_campaign_name,',
            '                   min(TIMESTAMP) AS min_exposure',
            '            FROM mapbox_customer_data.segment_pages',
            "            WHERE dt >= '2018-07-01'",
            '              AND context_campaign_name IS NOT NULL',
            '            GROUP BY 1,',
            '                     2) c ON a.anonymous_id = c.anon_id_ad',
            '         LEFT JOIN',
            '           (SELECT DISTINCT anonymous_id AS anon_id_event,',
            '                            original_timestamp,',
            '                            event,',
            '                            context_traits_email',
            '            FROM mapbox_customer_data.segment_tracks',
            "            WHERE dt >= '2018-07-01'",
            "              AND event LIKE 'submitted_%form'",
            '              AND context_traits_email IS NOT NULL ) d ON a.anonymous_id = d.anon_id_event)',
            '      WHERE context_campaign_name IS NOT NULL )',
            '   LIMIT 0) T']


def test_get_sub_query(unbundled, query_list):
    assert unbundled.get_sub_query(query_list)[0] == ['SELECT *']
    assert len(unbundled.get_sub_query(query_list)) == 2


def test_divider(unbundled, query_list):
    assert unbundled._divider(query_list)[0] == ['FROM',
                                                '  (SELECT *',
                                                '   FROM',
                                                '     (SELECT *',
                                                '      FROM',
                                                '        (SELECT a.*,',
                                                '                b.*,',
                                                '                c.*,',
                                                '                d.*',
                                                '         FROM',
                                                '           (SELECT DISTINCT anonymous_id,',
                                                '                            user_id',
                                                '            FROM mapbox_customer_data.segment_identifies',
                                                "            WHERE dt >= '2018-07-01'",
                                                '              AND anonymous_id IS NOT NULL',
                                                '              AND user_id IS NOT NULL ) a',
                                                '         LEFT JOIN',
                                                '           (SELECT id,',
                                                '                   email,',
                                                '                   created',
                                                '            FROM mapbox_customer_data.accounts',
                                                "            WHERE cast(dt AS DATE) = CURRENT_DATE - INTERVAL '1' DAY ) b ON a.user_id = b.id",
                                                '         LEFT JOIN',
                                                '           (SELECT anonymous_id AS anon_id_ad,',
                                                '                   context_campaign_name,',
                                                '                   min(TIMESTAMP) AS min_exposure',
                                                '            FROM mapbox_customer_data.segment_pages',
                                                "            WHERE dt >= '2018-07-01'",
                                                '              AND context_campaign_name IS NOT NULL',
                                                '            GROUP BY 1,',
                                                '                     2) c ON a.anonymous_id = c.anon_id_ad',
                                                '         LEFT JOIN',
                                                '           (SELECT DISTINCT anonymous_id AS anon_id_event,',
                                                '                            original_timestamp,',
                                                '                            event,',
                                                '                            context_traits_email',
                                                '            FROM mapbox_customer_data.segment_tracks',
                                                "            WHERE dt >= '2018-07-01'",
                                                "              AND event LIKE 'submitted_%form'",
                                                '              AND context_traits_email IS NOT NULL ) d ON a.anonymous_id = d.anon_id_event)',
                                                '      WHERE context_campaign_name IS NOT NULL )',
                                                '   LIMIT 0) T']


def test_parse_alias(unbundled, main_query, sub_query):
    assert unbundled._parse_alias(main_query, sub_query) == (['SELECT *', 'FROM', 'T'],
                                                            {'T': "SELECT * FROM (SELECT * FROM (SELECT a.*, b.*, c.*, d.* FROM (SELECT DISTINCT anonymous_id, user_id FROM mapbox_customer_data.segment_identifies WHERE dt >= '2018-07-01' AND anonymous_id IS NOT NULL AND user_id IS NOT NULL ) a LEFT JOIN (SELECT id, email, created FROM mapbox_customer_data.accounts WHERE cast(dt AS DATE) = CURRENT_DATE - INTERVAL '1' DAY ) b ON a.user_id = b.id LEFT JOIN (SELECT anonymous_id AS anon_id_ad, context_campaign_name, min(TIMESTAMP) AS min_exposure FROM mapbox_customer_data.segment_pages WHERE dt >= '2018-07-01' AND context_campaign_name IS NOT NULL GROUP BY 1, 2) c ON a.anonymous_id = c.anon_id_ad LEFT JOIN (SELECT DISTINCT anonymous_id AS anon_id_event, original_timestamp, event, context_traits_email FROM mapbox_customer_data.segment_tracks WHERE dt >= '2018-07-01' AND event LIKE 'submitted_%form' AND context_traits_email IS NOT NULL ) d ON a.anonymous_id = d.anon_id_event) WHERE context_campaign_name IS NOT NULL ) LIMIT 0"})


def test_stitch_main(unbundled, main_query, sub_query):
    assert unbundled._stitch_main(main_query, sub_query) == (['SELECT *', 'FROM', 'T'],
                                                            {'T': "SELECT * FROM (SELECT * FROM (SELECT a.*, b.*, c.*, d.* FROM (SELECT DISTINCT anonymous_id, user_id FROM mapbox_customer_data.segment_identifies WHERE dt >= '2018-07-01' AND anonymous_id IS NOT NULL AND user_id IS NOT NULL ) a LEFT JOIN (SELECT id, email, created FROM mapbox_customer_data.accounts WHERE cast(dt AS DATE) = CURRENT_DATE - INTERVAL '1' DAY ) b ON a.user_id = b.id LEFT JOIN (SELECT anonymous_id AS anon_id_ad, context_campaign_name, min(TIMESTAMP) AS min_exposure FROM mapbox_customer_data.segment_pages WHERE dt >= '2018-07-01' AND context_campaign_name IS NOT NULL GROUP BY 1, 2) c ON a.anonymous_id = c.anon_id_ad LEFT JOIN (SELECT DISTINCT anonymous_id AS anon_id_event, original_timestamp, event, context_traits_email FROM mapbox_customer_data.segment_tracks WHERE dt >= '2018-07-01' AND event LIKE 'submitted_%form' AND context_traits_email IS NOT NULL ) d ON a.anonymous_id = d.anon_id_event) WHERE context_campaign_name IS NOT NULL ) LIMIT 0"})


def test_separator(unbundled, copy_query_list, main_query):
    assert unbundled.separator(copy_query_list, main_query) == ('SELECT * FROM T ',
                                                            [{'T': "SELECT * FROM (SELECT * FROM (SELECT a.*, b.*, c.*, d.* FROM (SELECT DISTINCT anonymous_id, user_id FROM mapbox_customer_data.segment_identifies WHERE dt >= '2018-07-01' AND anonymous_id IS NOT NULL AND user_id IS NOT NULL ) a LEFT JOIN (SELECT id, email, created FROM mapbox_customer_data.accounts WHERE cast(dt AS DATE) = CURRENT_DATE - INTERVAL '1' DAY ) b ON a.user_id = b.id LEFT JOIN (SELECT anonymous_id AS anon_id_ad, context_campaign_name, min(TIMESTAMP) AS min_exposure FROM mapbox_customer_data.segment_pages WHERE dt >= '2018-07-01' AND context_campaign_name IS NOT NULL GROUP BY 1, 2) c ON a.anonymous_id = c.anon_id_ad LEFT JOIN (SELECT DISTINCT anonymous_id AS anon_id_event, original_timestamp, event, context_traits_email FROM mapbox_customer_data.segment_tracks WHERE dt >= '2018-07-01' AND event LIKE 'submitted_%form' AND context_traits_email IS NOT NULL ) d ON a.anonymous_id = d.anon_id_event) WHERE context_campaign_name IS NOT NULL ) LIMIT 0"}])


def test_delevel(unbundled, sample_query):
    assert unbundled.delevel(sample_query) == ('SELECT * FROM T ',
                                            [{'T': "SELECT * FROM (SELECT * FROM (SELECT a.*, b.*, c.*, d.* FROM (SELECT DISTINCT anonymous_id, user_id FROM mapbox_customer_data.segment_identifies WHERE dt >= '2018-07-01' AND anonymous_id IS NOT NULL AND user_id IS NOT NULL ) a LEFT JOIN (SELECT id, email, created FROM mapbox_customer_data.accounts WHERE cast(dt AS DATE) = CURRENT_DATE - INTERVAL '1' DAY ) b ON a.user_id = b.id LEFT JOIN (SELECT anonymous_id AS anon_id_ad, context_campaign_name, min(TIMESTAMP) AS min_exposure FROM mapbox_customer_data.segment_pages WHERE dt >= '2018-07-01' AND context_campaign_name IS NOT NULL GROUP BY 1, 2) c ON a.anonymous_id = c.anon_id_ad LEFT JOIN (SELECT DISTINCT anonymous_id AS anon_id_event, original_timestamp, event, context_traits_email FROM mapbox_customer_data.segment_tracks WHERE dt >= '2018-07-01' AND event LIKE 'submitted_%form' AND context_traits_email IS NOT NULL ) d ON a.anonymous_id = d.anon_id_event) WHERE context_campaign_name IS NOT NULL ) LIMIT 0"}])


def test_restructure_subquery(unbundled, sample_query):
    query_dict = {}
    if unbundled.has_child(sample_query):
        query_dict, _ = unbundled.restructure_subquery(query_dict, 'main', sample_query)
    else: 
        query_dict['main'] = sample_query

    assert query_dict == {'main': 'SELECT * FROM T '}

