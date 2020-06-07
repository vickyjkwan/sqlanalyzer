from sqlanalyzer import column_parser
import pandas as pd
import sqlparse
import re


def delevel_query(query_list):

    pos_delete = [len(query_list)-1]
    pos_join = []
    for i, line in enumerate(query_list):
        if line.startswith('ORDER') or line.startswith('GROUP'):
            pos_delete.append(i)
        if line.startswith('FROM') and len(line.split(' ')) > 1:
            pos_join.append(i)
        elif line.startswith('FROM') and len(line.split(' ')) == 1:
            pos_join.append(i+1)
        if line.startswith('WHERE'):
            pos_where = i
        if line.startswith('LEFT JOIN') or line.startswith('INNER JOIN') or line.startswith('FULL OUTER JOIN'):
            pos_join.append(i+1)

    pos_join.append(min(pos_delete))

    sub_query = {}
    pos_join_list = iter(pos_join)
    next(pos_join_list)
    alias_pos = []

    if query_list[pos_join[0]].startswith('FROM'):
        alias_pos.append(pos_join[0])

    for i in range(len(pos_join)-1):
        if i < len(pos_join)-2 and pos_join[i] < pos_where:
            end_pos = next(pos_join_list)-1
            alias_pos.append(end_pos-1)

        elif pos_join[-1] >= pos_where:
            end_pos = next(pos_join_list)-1
            alias_pos.append(pos_where - 1)

        else:
            end_pos = pos_join[-1]

    alias_pos = list(set(alias_pos))
    
    sub_query = {}

    for j in alias_pos:
        alias = query_list[j]
        try:
            alias_index = alias.split(' ')[::-1].index('ON')
            alias = alias.split(' ')[::-1][alias_index+1]
        except:
            alias = alias.split(' ')[-1]

        sub_query[alias] = ' '.join(query_list[pos_join[i] : end_pos])
            
    return sub_query


def main():
    query = """SELECT sfdc_accounts.platform, sfdc_accounts.mobile_os, sfdc_accounts.service_metadata,
            sfdc_cases.account, sfdc_cases.num_requests, sfdc_cases.owner
            FROM sfdc.accounts sfdc_accounts
            LEFT JOIN (SELECT MAX(dt) FROM (SELECT dt FROM sfdc.oppty) sfdc_oppty LEFT JOIN (SELECT dt FROM sfdc.cases) sfdc_cases ON sfdc_oppty.dt = sfdc_cases.dt) 
            AS sfdc_cases_oppty ON sfdc_cases_oppty.dt = sfdc_accounts.dt
            WHERE sfdc_cases_oppty.dt > '2020-04-03' AND sfdc_cases_oppty.dt < '2020-05-04' ORDER BY 1 GROUP BY 3 LIMIT 20
            """

    formatter = column_parser.Parser(query)
    formatted_query = formatter.format_query(query)
    query_list_0 = formatted_query.split('\n')

    sub_query = delevel_query(query_list_0)

    return sub_query


if __name__ == '__main__':
    print(main())