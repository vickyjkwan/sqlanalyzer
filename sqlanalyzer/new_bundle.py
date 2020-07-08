from sqlanalyzer import column_parser
import re
import json


def within(num, rng):
    if num >= min(rng) and num <= max(rng) and min(rng) < max(rng): return 1
    else: return 0


def is_cte(query):
    return query.startswith('WITH')


def clean_dict(query_dict):

    for k,v in query_dict.items(): 
        if isinstance(v, dict) and len(v.keys()) == 1 and 'main' in v.keys():
            query_dict[k] = v['main']
            
    return query_dict


def main_query(query_list, sub_query_pos):

    l = []
    for i in range(len(query_list)): 
        count = 0
        for pair in sorted(sub_query_pos):
            count += within(i, pair)
        if count == 0:
            l.append(i)

    return l


def delevel(query_list):
    copy_query_list = get_sub_query(query_list)
    join_dict = get_sub_dict(copy_query_list)

    return join_dict


def has_child(formatted_query):

    count = 0
    deleveled_list = delevel(formatted_query.split('\n'))
    if len(deleveled_list.keys()) > 1:
        for _,v in deleveled_list.items():
            if v != {}: count += 1

    return count != 0


## get sub query dict

def get_sub_query(query_list):
    pos_delete, pos_where = [len(query_list)-1], len(query_list)

    for i, line in enumerate(query_list):
        if line.startswith('ORDER') or line.startswith('GROUP'):
            pos_delete.append(i)    
        elif line.startswith('WHERE'):
            pos_where = i

    end_of_query = min(pos_delete) 
    
    copy_query_list = query_list.copy()
    main = next((s for s in copy_query_list if s.startswith('FROM')), 'end of query')
    main_pos = copy_query_list.index(main)
    main_query = copy_query_list[:main_pos+1]
    main_query.extend(copy_query_list[pos_where:end_of_query])
    del copy_query_list[:main_pos]
    del copy_query_list[(pos_where-main_pos):]
    
    return copy_query_list


def landmark(line):
    for syntax in ['FROM', 'LEFT JOIN', 'INNER JOIN', 'OUTER JOIN', 'RIGHT JOIN', 'CROSS JOIN', 'FULL JOIN', 'FULL OUTER JOIN']:
        if line.startswith(syntax):
            return True
        

def divide(copy_query_list):
    sub_join = []
    for i, line in enumerate(copy_query_list): 

        if landmark(line):
            sub_join.append(line)
            del copy_query_list[:i+1]
            join_query = next((s for s in copy_query_list if not s.startswith(' ')), 'end of query')
            try:
                join_pos = copy_query_list.index(join_query)
                sub_join.extend(copy_query_list[:join_pos])
                del copy_query_list[:join_pos]
                break
            except: 
                sub_join.extend(copy_query_list)
                del copy_query_list[:]
                break

    return sub_join, copy_query_list


def get_sub_dict(copy_query_list):
    i = 0
    join_dict = {}
    new_copy_query_list = copy_query_list
    sub_join = [1]
    while sub_join != []:
        i += 1
        sub_join, new_copy_query_list = divide(new_copy_query_list)
        if sub_join != []:
            join_dict['join_{}'.format(i)] = '\n'.join(sub_join)
            
    return join_dict



