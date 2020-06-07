from sqlanalyzer import column_parser
import pandas as pd
import sqlparse
import re


def get_joins_pos(query_list):

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
    return pos_join, pos_where


def get_alias_pos(query_list, pos_join, pos_where):
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

        elif pos_where == len(query_list):
            end_pos = pos_join[-1]
            alias_pos.append(pos_where-1)
        # else:
        #     end_pos = pos_join[-1]

    alias_pos = list(set(alias_pos))
    return alias_pos


def parse_sub_query(query_list, sub_query_pos):
    sub_query = {}
    for _, sub_pos in enumerate(sub_query_pos):
        alias = query_list[sub_pos[1]]
        query = query_list[sub_pos[0]: sub_pos[1]]

        try:
            alias_list_rev = alias.split(' ')[::-1]
            alias_index = alias_list_rev.index('ON')
            alias = alias_list_rev[alias_index+1]

            if alias_list_rev[alias_index+2] == 'AS':
                del alias_list_rev[:alias_index+3]

            else:
                del alias_list_rev[:alias_index+2]

            query.append(' '.join(alias_list_rev[::-1]).rstrip(r'\)').lstrip(' '))

        except:
            query.append(' '.join(alias.split(' ')[:-1]).rstrip(r'\)').lstrip(' '))
            alias = alias.split(' ')[-1]

        sub_query[alias] = ' '.join(query).lstrip(' \(')
        
    return sub_query


def delevel(query_list):

    sub_query = {}
    pos_join, pos_where = get_joins_pos(query_list)
    alias_pos = get_alias_pos(query_list, pos_join, pos_where)
    sub_query_pos = list(zip(pos_join[:-1], alias_pos))
    sub_query = parse_sub_query(query_list, sub_query_pos)

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

    sub_query = delevel(query_list_0)

    return sub_query


if __name__ == '__main__':
    print(main())