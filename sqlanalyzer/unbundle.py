from sqlanalyzer import column_parser
import re
import json


def within(num, rng):
    if num >= min(rng) and num <= max(rng) and min(rng) < max(rng): return 1
    else: return 0


def is_cte(query):
    return query.startswith('WITH')


def landmark(line):
    for syntax in ['FROM', 'LEFT JOIN', 'INNER JOIN', 'OUTER JOIN', 'RIGHT JOIN', 'CROSS JOIN', 'FULL JOIN', 'FULL OUTER JOIN']:
        if line.startswith(syntax):
            return True


def has_child(sub_query):
    if 'SELECT' in sub_query:
        return True
    else: 
        return False


def clean_dict(query_dict):
    for k,v in query_dict.items(): 
        if isinstance(v, dict) and len(v.keys()) == 1 and 'main' in v.keys():
            query_dict[k] = v['main']
            
    return query_dict


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
    main_query = copy_query_list[:main_pos]
    main_query.extend(copy_query_list[pos_where:end_of_query])
    del copy_query_list[:main_pos]
    del copy_query_list[(pos_where-main_pos):]
    
    return main_query, copy_query_list
        

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
    

def parse_alias(main_query, sub_query):
    sub_query_list = sub_query.rstrip('\n ').split(' ')
    sub_query_list = [w for w in sub_query_list if w]
    sub_query_dict = {}
    
    if sub_query_list[0] == 'FROM':
        
        main_query.append('FROM')
        sub_query_list.pop(0)
        
        sub_query_list_rev = sub_query_list[::-1]
        
        if sub_query_list_rev[0] != ')':
            alias = sub_query_list_rev[0]
            sub_query_list.pop()
            
        else:
            alias = 'no alias'
    
    elif sub_query_list[0].rstrip('\n ') not in ('FROM', 'CROSS'):
        
        join_ind = sub_query_list.index('JOIN')
        main_query.extend(sub_query_list[:join_ind+1] )
        del sub_query_list[:join_ind+1] 

        sub_query_list_rev = sub_query_list[::-1]

        try: 
            on_ind = sub_query_list_rev.index('ON')
            alias = sub_query_list_rev[on_ind+1]

        except ValueError:
            if sub_query_list_rev[0] != ')':
                alias = sub_query_list_rev[0]
            else:
                alias = 'no alias'
    
    elif sub_query_list[0].rstrip('\n ') == 'CROSS':
        
        join_ind = sub_query_list.index('JOIN')
        main_query.extend(sub_query_list[:join_ind+1] )
        del sub_query_list[:join_ind+1] 

        sub_query_list_rev = sub_query_list[::-1]
        try:
            as_ind = sub_query_list_rev.index('AS')
            alias = sub_query_list_rev[as_ind-1]

        except ValueError:
            alias = sub_query_list_rev[0]
    
    main_query.append(alias)
    sub_query_dict[alias] = ' '.join(sub_query_list).lstrip('(').rstrip(')')
    return main_query, sub_query_dict


def stitch_main(main_query, sub_query):
    sub_query_dict = {}
    if has_child(sub_query):
        main_query, sub_query_dict = parse_alias(main_query, sub_query)
    else:
        main_query.append(sub_query)
        
    return main_query, sub_query_dict
    

def separator(copy_query_list, main_query):
    sub_query_list_copy = copy_query_list
    sub_query = 'abc'
    sub_queries = []

    while sub_query:
        sub_join, sub_query_list_copy = divide(sub_query_list_copy)
        sub_query = ' '.join(sub_join)
        main_query, sub_query_dict = stitch_main(main_query, sub_query)
        if sub_query_dict != {}: sub_queries.append(sub_query_dict)
    
    return ' '.join(main_query), sub_queries


def main():

    query = open('../sample_query.sql').read()

    formatter = column_parser.Parser(query)
    formatted_query = formatter.format_query(query)
    query_list_0 = formatted_query.split('\n')

    main_query, copy_query_list = get_sub_query(query_list_0)

    main_query, sub_queries = separator(copy_query_list, main_query)

    return main_query, sub_queries


if __name__ == '__main__':
    print(main())