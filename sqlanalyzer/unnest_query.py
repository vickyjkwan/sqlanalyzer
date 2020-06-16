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

    if min(pos_delete) == len(query_list)-1:
        pos_join.append(min(pos_delete))
    else:
        pass

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
        
        else:
            end_pos = pos_join[-1]
            alias_pos.append(pos_where-1)

    alias_pos = sorted(list(set(alias_pos)))

    return alias_pos


def parse_sub_query(query_list, sub_query_pos):

    sub_query = {}
    keep = []
    for _, sub_pos in enumerate(sub_query_pos):
        alias = query_list[sub_pos[1]]
        query = query_list[sub_pos[0]: sub_pos[1]]

        try:
            alias_list_rev = alias.split(' ')[::-1]
            if alias_list_rev[0][-1] != ')':
                alias_index = alias_list_rev.index('ON')
                alias = alias_list_rev[alias_index+1]

                if alias_list_rev[alias_index+2] == 'AS':
                    keep.append(' '.join(alias_list_rev[:alias_index+3][::-1]))
                    del alias_list_rev[:alias_index+3]

                else:
                    keep.append(' '.join(alias_list_rev[:alias_index+2][::-1]))
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
        
    return sub_query, keep


def delevel(query_list):

    sub_query = {}
    pos_join, pos_where = get_joins_pos(query_list)
    alias_pos = get_alias_pos(query_list, pos_join, pos_where)
    sub_query_pos = list(zip(pos_join, alias_pos))
    sub_query, keep = parse_sub_query(query_list, sub_query_pos)
    main_query_pos = main_query(query_list, sub_query_pos)
    if main_query_pos != []:
        sub_query['main'] = '\n'.join([query_list[p] for p in main_query_pos])
    sub_query['main'] = sub_query['main'] + ' ' + '\n'.join(keep)
    
    return sub_query


def has_child(formatted_query):
    
    count = 0
    if len(delevel(formatted_query.split('\n')).keys()) > 1:
        for _,v in delevel(formatted_query.split('\n')).items():
            if v != {}: count += 1
            
    return count != 0


def within(num, rng):
    if num >= min(rng) and num <= max(rng): return 1
    else: return 0

    
def main_query(query_list, sub_query_pos):
    l = []
    for i in range(len(query_list)): 
        count = 0
        for pair in sorted(sub_query_pos):
            count += within(i, pair)
        if count == 0:
            l.append(i)
    return l
        
    
def clean_dict(query_dict):
    for k,v in query_dict.items(): 
        if isinstance(v, dict) and len(v.keys()) == 1 and 'main' in v.keys():
            query_dict[k] = v['main']
    return query_dict



def main(query):

    formatter = column_parser.Parser(query)
    formatted_query = formatter.format_query(query)
    query_list_0 = formatted_query.split('\n')
    query_dict = {}
    sub_query = delevel(query_list_0)
    query_dict = sub_query

    for alias, query in sub_query.items():

        formatter = column_parser.Parser(query)
        formatted_query = formatter.format_query(query)
        query_list = formatted_query.split('\n')

        if has_child(formatted_query) and alias != 'main':
            sub_query_dict = delevel(query_list)
            query_dict[alias] = sub_query_dict
            query_dict = clean_dict(query_dict)

            for alias2, query2 in sub_query_dict.items():
                formatter2 = column_parser.Parser(query2)
                formatted_query2 = formatter2.format_query(query2)
                query_list2 = formatted_query2.split('\n')
                
                if has_child(formatted_query2) and alias2 != 'main':
                    sub_query_dict2 = delevel(query_list2)
                    sub_query_dict[alias2] = sub_query_dict2
                    
                    
                    for alias3, query3 in sub_query_dict2.items():
                        formatter3 = column_parser.Parser(query3)
                        formatted_query3 = formatter3.format_query(query3)
                        query_list3 = formatted_query3.split('\n')
                        
                        if has_child(formatted_query3) and alias3 != 'main':
                            sub_query_dict3 = delevel(query_list3)
                            sub_query_dict2[alias3] = sub_query_dict3
                            
                            for alias4, query4 in sub_query_dict3.items():
                                formatter4 = column_parser.Parser(query4)
                                formatted_query4 = formatter4.format_query(query4)
                                query_list4 = formatted_query4.split('\n')
                                
                                if has_child(formatted_query4) and alias4 != 'main':
                                    sub_query_dict4 = delevel(query_list4)
                                    sub_query_dict3[alias4] = sub_query_dict4
                                else:
                                    pass
                            query_dict[alias][alias2][alias3] = sub_query_dict3
                            query_dict = clean_dict(query_dict)
                        else:
                            pass
                        query_dict[alias][alias2] = sub_query_dict2
                        query_dict = clean_dict(query_dict)
                else:
                    pass
                query_dict[alias] = sub_query_dict
                query_dict = clean_dict(query_dict)
        else:
            pass


    return query_dict


def is_cte(query):
    return query.startswith('WITH')


if __name__ == '__main__':
    #### BUG: line 45 `FROM sfdc.oppty` has no alias and wasn't showing #### 
    # query = open('query.sql').read()
    query = open('long_query.sql').read()
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
                final_dict[alias] = main(formatted_query)
            except:
                final_dict[alias] = formatted_query

        # with open('data.json', 'w') as outfile:
        #     json.dump(final_dict, outfile)
        print(json.dumps(final_dict, indent=2), '\n\n\n')

    else:
        print(json.dumps(main(query), indent=2))
    # else:
    #     with open('data.json', 'w') as outfile:
    #         json.dump(main(query), outfile)
