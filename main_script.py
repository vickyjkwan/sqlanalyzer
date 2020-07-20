from sqlanalyzer import column_parser, unbundle, query_analyzer
import sqlparse
import re, json, time, sys
import pandas as pd


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
    
    raw_query = open('queries/{}.sql'.format(sys.argv[1])).read()
    analyzer = query_analyzer.Analyzer(raw_query)
    print(analyzer.parse_query(raw_query))


# need to audit `FROM a, b`: comma joins
# 0.15 seconds 53 lines 
# query = open('query.sql').read()
# 0.34 seconds 124 lines 
# query = open('long_query.sql').read()
# 1.59 seconds 455 lines: 0.34 seconds/100 lines 
# query = open('test_query.sql').read()

# query = open('api_requests_subunits.sql').read().replace('{cloudfront_logs_dataset}', 'logs.cloudfront_logs')\
#     .replace('{cloudfront_logs_china_dataset}', 'logs.cloudfront_logs_china')\
#         .replace('{cloudfront_logs_china_to_global_proxy_dataset}', 'logs.cloudfront_logs_china_to_global_proxy')
            
# t0 = time.perf_counter()


# with open('./data.json', 'r') as f:
#     query_dict = json.load(f)

# db_fields_1 = pd.DataFrame({'db_table': 'mapbox_customer_data.accounts', 
#         'all_columns': ['id', 'accountlevel', 'email', 'created', 'service_metadata_version', 'account', 'num_requests', 'dt', 'customerid']})
# db_fields_2 = pd.DataFrame({'db_table': 'analytics.mbx_account_verticals', 
#         'all_columns': ['mapbox_account_id', 'email', 'salesforce_account_id', 'vertical', 'vertical_source', 'dt']})
# db_fields_3 = pd.DataFrame({'db_table': 'sfdc.mapbox_accounts', 
#         'all_columns': ['mapbox_account_id', 'mapbox_account_salesforce_id', 'salesforce_account_id', 'dt']})
# db_fields_4 = pd.DataFrame({'db_table': 'sfdc.accounts', 
#         'all_columns': ['name', 'dt', 'account_health_c', 'billing_state']})
# db_fields_5 = pd.DataFrame({'db_table': 'analytics.apa_deals',
#                        'all_columns': ['mbx_acct_id', 'prd_start_date', 'prd_end_date', 'dt', 'stripe_cust_id']})
# db_fields_6 = pd.DataFrame({'db_table': 'analytics.ref_calendar',
#                        'all_columns': ['vdate']})
# db_fields_7 = pd.DataFrame({'db_table': 'payments.stripe_invoices',
#                        'all_columns': ['date', 'discount_id', 'customer_id', 'received_at', 'amount_due']})
# db_fields_8 = pd.DataFrame({'db_table': 'payments.stripe_discounts',
#                        'all_columns': ['id', 'coupon_id', 'subscription', 'received_at']})
# db_fields_9 = pd.DataFrame({'db_table': 'payments.stripe_coupons',
#                        'all_columns': ['id', 'duration', 'percent_off', 'created', 'currency']})
# db_fields_10 = pd.DataFrame({'db_table': 'analytics.service_endpoint_mapping',
#                        'all_columns': ['parent_sku', 'sku_id', 'service_org', 'dt', 'platform']})
# db_fields_11 = pd.DataFrame({'db_table': 'analytics.api_requests',
#                        'all_columns': ['log_source', 'platform', 'service', 'country',
#                                       'status_code', 'request_type', 'plugin', 'account', 
#                                       'hr', 'num_requests', 'dt', 'hr_segment']})
# db_fields_12 = pd.DataFrame({'db_table': 'analytics.service_endpoint_mapping',
#                        'all_columns': ['parent_sku', 'sku_id', 'service_org', 'dt', 'platform']})
# df = db_fields_1.append(db_fields_2, ignore_index=True)
# df = df.append(db_fields_3, ignore_index=True)
# df = df.append(db_fields_4, ignore_index=True)
# df = df.append(db_fields_5, ignore_index=True)
# df = df.append(db_fields_6, ignore_index=True)
# df = df.append(db_fields_7, ignore_index=True)
# df = df.append(db_fields_8, ignore_index=True)
# df = df.append(db_fields_9, ignore_index=True)
# df = df.append(db_fields_10, ignore_index=True)
# df = df.append(db_fields_11, ignore_index=True)
# df = df.append(db_fields_12, ignore_index=True)


# for k,v in query_dict.items():
#     if isinstance(v, dict):
#         print(k, '\n', compile_queried_cols(v, df), '\n\n')
#     else:
#         print(k, '\n', extract_subquery_fields(v, df), '\n\n')

# t1 = time.perf_counter()
# print(t1-t0)