from sqlanalyzer import column_parser, unbundle
from sqlanalyzer.unbundle import *
import sqlparse
import re
import json
import pandas as pd


def extract_query_dict(query):

    formatter = column_parser.Parser(query)
    formatted_query = formatter.format_query(query)
    query_list_0 = formatted_query.split('\n')
    query_dict = {}

    unbundled = unbundle.Unbundle(query)
    sub_query = unbundled.delevel(query_list_0)
    query_dict = sub_query

    for alias, query in sub_query.items():

        formatter = column_parser.Parser(query)
        formatted_query = formatter.format_query(query)
        query_list = formatted_query.split('\n')

        if unbundled.has_child(formatted_query) and alias != 'main':
            sub_query_dict = unbundled.delevel(query_list)
            query_dict[alias] = sub_query_dict
            query_dict = clean_dict(query_dict)

        else:
            pass

    return query_dict


def extract_subquery_fields(query, db_fields):
    formatter = column_parser.Parser(query)
    formatted = formatter.format_query(query)
    fields = formatter.match_queried_fields(formatted, db_fields)
    return fields


def compile_queried_cols(query_dict, df):
    all_cols = []
    for _,v in query_dict.items():
        if isinstance(v, dict):
            for _,v1 in v.items():
                all_cols.extend(extract_subquery_fields(v1, df))
        else:
            all_cols.extend(extract_subquery_fields(v, df))
    return all_cols


if __name__ == '__main__':

    # query = open('query.sql').read()
    # query = open('long_query.sql').read()
    #### BUG: nested was not detected ####
    query = open('test_query.sql').read()
    formatter = column_parser.Parser(query)
    formatted_query = formatter.format_query(query)
    query_list = formatted_query.split('\n')

    if is_cte(formatted_query):
        cte_dict = formatter.parse_cte(formatted_query)
        final_dict = {}
        for alias, query in cte_dict.items():
            formatter = column_parser.Parser(query)
            formatted_query = formatter.format_query(query)
            try:
                final_dict[alias] = extract_query_dict(formatted_query)
            except:
                final_dict[alias] = formatted_query

        # with open('data.json', 'w') as outfile:
        #     json.dump(final_dict, outfile)

        print(final_dict)

    else:
        # with open('data.json', 'w') as outfile:
        #     json.dump(extract_query_dict(query), outfile)
        print(extract_query_dict(query))


    # with open('./data.json', 'r') as f:
    #     query_dict = json.load(f)

    # db_fields_1 = pd.DataFrame({'db_table': 'mapbox_customer_data.segment_identifies', 
    #         'all_columns': ['anonymous_id', 'user_id', 'service', 'service_metadata', 'service_metadata_version', 'account', 'num_requests', 'dt']})
    # db_fields_2 = pd.DataFrame({'db_table': 'mapbox_customer_data.accounts', 
    #             'all_columns': ['id', 'user_id', 'email', 'created', 'service_metadata_version', 'account', 'num_requests', 'dt']})
    # db_fields_3 = pd.DataFrame({'db_table': 'mapbox_customer_data.segment_pages', 
    #         'all_columns': ['anonymous_id', 'context_campaign_name', 'service', 'service_metadata', 'service_metadata_version', 'account', 'num_requests', 'dt']})
    # db_fields_4 = pd.DataFrame({'db_table': 'mapbox_customer_data.segment_tracks', 
    #         'all_columns': ['anonymous_id', 'original_timestamp', 'event', 'context_traits_email', 'service_metadata_version', 'account', 'num_requests', 'dt']})
    # db_fields_5 = pd.DataFrame({'db_table': 'sfdc.cases', 
    #         'all_columns': ['account', 'num_requests', 'owner', 'anonymous_id', 'id', 'original_timestamp', 'event', 'context_traits_email', 'service_metadata_version', 'dt']})
    # db_fields_6 = pd.DataFrame({'db_table': 'sfdc.owner',
    #                        'all_columns': ['dt', 'first_name', 'last_name']})
    # db_fields_7 = pd.DataFrame({'db_table': 'sfdc.accounts',
    #                        'all_columns': ['platform', 'case_id', 'mobile_os', 'service_metadata', 'user_id', 'first_name', 'last_name']})
    # df = db_fields_1.append(db_fields_2, ignore_index=True)
    # df = df.append(db_fields_3, ignore_index=True)
    # df = df.append(db_fields_4, ignore_index=True)
    # df = df.append(db_fields_5, ignore_index=True)
    # df = df.append(db_fields_6, ignore_index=True)
    # df = df.append(db_fields_7, ignore_index=True)

    # for k,v in query_dict.items():
    #     if isinstance(v, dict):
    #         print(k, '\n', compile_queried_cols(v, df), '\n\n')
    #     else:
    #         print(k, '\n', extract_subquery_fields(v, df), '\n\n')