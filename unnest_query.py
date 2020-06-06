from sqlanalyzer import column_parser
import pandas as pd
import sqlparse
import re


def delevel_query(query_list):

    sub_query = []
    pos_delete = []
    pos_join = []
    for i, line in enumerate(query_list):
        if line.startswith('ORDER') or line.startswith('GROUP'):
            pos_delete.append(i)
        if line.startswith('FROM') and len(line.split(' ')) > 1:
            print(line)
            pos_join.append(i)
        elif line.startswith('FROM') and len(line.split(' ')) == 1:
            pos_join.append(i+1)
        if line.startswith('LEFT JOIN') or line.startswith('INNER JOIN') or line.startswith('FULL OUTER JOIN'):
            pos_join.append(i+1)

    pos_join.append(pos_delete[0])    

    sub_query = {}
    pos_join_list = iter(pos_join)
    next(pos_join_list)

    for i in range(len(pos_join)-1):
        if i < len(pos_join)-2:
            sub_query['sub_query_{}'.format(i)] = ' '.join(query_list[pos_join[i] : next(pos_join_list)-1])
        else:
            sub_query['sub_query_{}'.format(i)] = ' '.join(query_list[pos_join[i] : pos_join[-1]])
            
    return sub_query



def parse_sub_query(sub_query_list):
    sub_query = "\n".join(sub_query_list)
    sub_query = sub_query.lstrip('\n').lstrip(' ')
    
    formatter = column_parser.Parser(sub_query)
    formatted = formatter.format_query(sub_query)
    sub_query_list = formatted.split('\n')

    return sub_query_list


def has_child(sub_query_list):
    query_list = parse_sub_query(sub_query_list)
    query_list = delevel_query(query_list)[1]
    return query_list, query_list != []


def main():
    query = """SELECT * FROM sfdc.accounts sfdc_accounts
            LEFT JOIN (SELECT MAX(dt) FROM (SELECT dt FROM sfdc.oppty sfdc_oppty) LEFT JOIN (SELECT dt FROM sfdc.cases)) AS sfdc_cases ON sfdc_cases.dt = sfdc_accounts.dt
            WHERE dt > '2020-04-03' 
            """

    formatter = column_parser.Parser(query)
    formatted = formatter.format_query(query)
    query_list = formatted.split('\n')

    has_subquery = True
    while has_subquery:
        sub_query_list, has_subquery = has_child(query_list)
        query_list = sub_query_list
        print(has_subquery, '\n'.join(query_list),  '\n')