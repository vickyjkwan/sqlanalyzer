from sqlanalyzer import column_parser
import pandas as pd
import sqlparse
import re
import json


def get_joins_pos(query_list):

    pos_delete, pos_where = [len(query_list)-1], len(query_list)
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
        else:
            end_pos = pos_join[-1]
            alias_pos.append(end_pos)

    alias_pos = sorted(list(set(alias_pos)))
    return alias_pos


def parse_sub_query(query_list, sub_query_pos):
    sub_query = {}
    for _, sub_pos in enumerate(sub_query_pos):
        alias = query_list[sub_pos[1]]
        query = query_list[sub_pos[0]: sub_pos[1]]

        try:
            alias_list_rev = alias.split(' ')[::-1]
            if alias_list_rev[0][-1] != ')':
                alias_index = alias_list_rev.index('ON')
                alias = alias_list_rev[alias_index+1]

                if alias_list_rev[alias_index+2] == 'AS':
                    del alias_list_rev[:alias_index+3]

                else:
                    del alias_list_rev[:alias_index+2]

                query.append(' '.join(alias_list_rev[::-1]).rstrip(r'\)').lstrip(' '))

            else:
                alias_list_rev[0] = alias_list_rev[0].rstrip(r'\)')
                alias = 'no alias'
                query.append(' '.join(alias_list_rev[::-1]))

        except:
            query.append(' '.join(alias.split(' ')[:-1]).rstrip(r'\)').lstrip(' '))
            alias = alias.split(' ')[-1]

        trans_query = ' '.join(query).lstrip(r' \(').lstrip(' FROM')
    
        if trans_query == '':
            sub_query = {}
        else:
            sub_query[alias] = trans_query
        
    return sub_query


def delevel(query_list):

    sub_query = {}
    pos_join, pos_where = get_joins_pos(query_list)
    alias_pos = get_alias_pos(query_list, pos_join, pos_where)
    sub_query_pos = list(zip(pos_join[:-1], alias_pos))
    sub_query = parse_sub_query(query_list, sub_query_pos)

    return sub_query


def has_child(formatted_query):
    
    count = 0
    for _,v in delevel(formatted_query.split('\n')).items():
        if v != {}: count += 1
            
    return count != 0



def main():
    # query = """SELECT sfdc_accounts.platform, sfdc_accounts.mobile_os, sfdc_accounts.service_metadata,
    #         sfdc_cases.account, sfdc_cases.num_requests, sfdc_cases.owner
    #         FROM sfdc.accounts sfdc_accounts
    #         LEFT JOIN (SELECT MAX(dt) FROM (SELECT dt FROM sfdc.oppty) sfdc_oppty LEFT JOIN (SELECT dt FROM sfdc.cases) sfdc_cases ON sfdc_oppty.dt = sfdc_cases.dt) 
    #         AS sfdc_cases_oppty ON sfdc_cases_oppty.dt = sfdc_accounts.dt
    #         WHERE sfdc_cases_oppty.dt > '2020-04-03' AND sfdc_cases_oppty.dt < '2020-05-04' ORDER BY 1 GROUP BY 3 LIMIT 20
    #         """

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
                LEFT JOIN (SELECT MAX(dt) FROM (SELECT dt FROM sfdc.oppty) sfdc_oppty LEFT JOIN (SELECT dt FROM sfdc.cases) sfdc_cases ON sfdc_oppty.dt = sfdc_cases.dt) 
                AS sfdc_cases_oppty ON sfdc_cases_oppty.dt = sfdc_accounts.dt
                WHERE sfdc_cases_oppty.dt > '2020-04-03' AND sfdc_cases_oppty.dt < '2020-05-04' ORDER BY 1 GROUP BY 3 LIMIT 20
                        ) e ON e.user_id = a.user_id
                        )
                WHERE context_campaign_name IS NOT NULL 
    """

    formatter = column_parser.Parser(query)
    formatted_query = formatter.format_query(query)
    query_list_0 = formatted_query.split('\n')
    query_dict = {}
    sub_query = delevel(query_list_0)
    query_dict['derived_query'] = sub_query

    for alias, query in sub_query.items():
        formatter = column_parser.Parser(query)
        formatted_query = formatter.format_query(query)
        query_list = formatted_query.split('\n')
        if has_child(formatted_query):
            sub_query_dict = delevel(query_list)
            query_dict['derived_query'][alias] = sub_query_dict
            
            for alias2, query2 in sub_query_dict.items():
                formatter2 = column_parser.Parser(query2)
                formatted_query2 = formatter2.format_query(query2)
                query_list2 = formatted_query2.split('\n')
                
                if has_child(formatted_query2):
                    sub_query_dict2 = delevel(query_list2)
                    sub_query_dict[alias2] = sub_query_dict2
                    
                    for alias3, query3 in sub_query_dict2.items():
                        formatter3 = column_parser.Parser(query3)
                        formatted_query3 = formatter3.format_query(query3)
                        query_list3 = formatted_query3.split('\n')
                        if has_child(formatted_query3):
                            sub_query_dict3 = delevel(query_list3)
                            sub_query_dict2[alias3] = sub_query_dict3
                            
                            for alias4, query4 in sub_query_dict3.items():
                                formatter4 = column_parser.Parser(query4)
                                formatted_query4 = formatter4.format_query(query4)
                                query_list4 = formatted_query4.split('\n')
                                if has_child(formatted_query4):
                                    sub_query_dict4 = delevel(query_list4)
                                    sub_query_dict3[alias4] = sub_query_dict4
                                else:
                                    pass
                        else:
                            pass
                else:
                    pass
        else:
            pass


    return query_dict


if __name__ == '__main__':
    print(json.dumps(main(), indent=2))
    