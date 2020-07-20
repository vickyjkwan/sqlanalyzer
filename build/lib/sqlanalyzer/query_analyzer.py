from sqlanalyzer import column_parser, unbundle
import sqlparse
import re, json, time, sys
import pandas as pd

class Analyzer:

    def __init__(self, raw_query=""):
        self.raw_query = raw_query


    def flatten_subquery(self, final_list, sub_queries, level_num):
        
        for q in sub_queries:
            for alias,query in q.items():
                formatter = column_parser.Parser(query)
                formatted_query = formatter.format_query(query)
                unbundled = unbundle.Unbundle(formatted_query)
                query_dict = {}
                if unbundled.has_child(query):
                    if alias == 'no alias' or alias == '' or alias == 'query':
                        query_dict, sub_queries = unbundled.restructure_subquery(query_dict, 'level_{}_main'.format(level_num), formatted_query)
                    else:
                        query_dict, sub_queries = unbundled.restructure_subquery(query_dict, alias, formatted_query)

            if query_dict != {}:
                final_list.append(query_dict)

            for subq in sub_queries:
                for _, sub_query in subq.items():
                    if not unbundled.has_child(sub_query): 
                        final_list.append(subq)
                        sub_queries.remove(subq)

        return final_list, sub_queries


    def flatten_pure_nested(self, query):

        sub_queries = [{'query': query}]
        final_list = []
        i = 0

        while sub_queries != []:
            i += 1
            final_list, sub_queries = self.flatten_subquery(final_list, sub_queries, level_num=i)

        return final_list


    def flatten_cte_nested(self, unbundled, cte_dict):

        cte_list = []

        for cte_alias, cte_query in cte_dict.items():
            if unbundled.has_child(cte_query):
                cte_list.append({cte_alias: self.flatten_pure_nested(cte_query)})
            else:
                cte_list.append({cte_alias: cte_query})

        return cte_list


    def parse_query(self, raw_query):

        formatter = column_parser.Parser(raw_query)
        formatted_query = formatter.format_query(raw_query)

        if 'WITH' in formatted_query:
            
            if formatted_query.startswith('WITH'):

                cte_dict = formatter.parse_cte(formatted_query)
                unbundled = unbundle.Unbundle(formatted_query)
                final_list = self.flatten_cte_nested(unbundled, cte_dict)
            
            else:
                sub_query_list = self.flatten_pure_nested(formatted_query)
                final_list = []
                for q in sub_query_list: 

                    for alias, query in q.items():
                        if 'WITH' in query:
                            formatter = column_parser.Parser(query)
                            formatted_query = formatter.format_query(query)

                            cte_dict = formatter.parse_cte(formatted_query)
                            unbundled = unbundle.Unbundle(formatted_query)

                            cte_list = self.flatten_cte_nested(unbundled, cte_dict)
                            final_list.append({alias: cte_list})

                        else:
                            final_list.append(q)

        else:
            final_list = self.flatten_pure_nested(raw_query)
        
        return final_list
