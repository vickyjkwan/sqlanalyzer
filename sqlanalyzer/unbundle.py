from sqlanalyzer import column_parser
import re
import json
import pandas as pd


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


class Unbundle:

    def __init__(self, raw_query=""):
        self.raw_query = raw_query


    def main_query(self, query_list, sub_query_pos):

        l = []
        for i in range(len(query_list)): 
            count = 0
            for pair in sorted(sub_query_pos):
                count += within(i, pair)
            if count == 0:
                l.append(i)

        return l
            

    def _get_joins_pos(self, query_list):

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
            if line.startswith('LEFT JOIN') or line.startswith('INNER JOIN') or line.startswith('FULL OUTER JOIN') or line.startswith('RIGHT JOIN'):
                pos_join.append(i+1)

        if min(pos_delete) == len(query_list)-1:
            pos_join.append(min(pos_delete))
        else:
            pass

        return pos_join, pos_where


    def _get_alias_pos(self, query_list, pos_join, pos_where):

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


    def _parse_sub_query(self, query_list, sub_query_pos):

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


    def delevel(self, query_list):

        sub_query = {}
        pos_join, pos_where = self._get_joins_pos(query_list)
        alias_pos = self._get_alias_pos(query_list, pos_join, pos_where)
        sub_query_pos = list(zip(pos_join, alias_pos))
        sub_query, keep = self._parse_sub_query(query_list, sub_query_pos)
        main_query_pos = self.main_query(query_list, sub_query_pos)
        if main_query_pos != []:
            sub_query['main'] = '\n'.join([query_list[p] for p in main_query_pos])
        sub_query['main'] = sub_query['main'] + ' ' + '\n'.join(keep)
        
        return sub_query


    def has_child(self, formatted_query):
        
        count = 0
        deleveled_list = self.delevel(formatted_query.split('\n'))
        if len(deleveled_list.keys()) > 1:
            for _,v in deleveled_list.items():
                if v != {}: count += 1
                
        return count != 0


    def extract_query_dict(self, query):

        formatter = column_parser.Parser(query)
        formatted_query = formatter.format_query(query)
        query_list_0 = formatted_query.split('\n')
        query_dict = {}

        sub_query = self.delevel(query_list_0)
        query_dict = sub_query

        for alias, query in sub_query.items():

            formatter = column_parser.Parser(query)
            formatted_query = formatter.format_query(query)
            query_list = formatted_query.split('\n')

            if self.has_child(formatted_query) and alias != 'main':
                sub_query_dict = self.delevel(query_list)
                query_dict[alias] = sub_query_dict
                query_dict = clean_dict(query_dict)

            else:
                pass

        return query_dict