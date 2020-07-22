import re
import json
import logging
import json
import pkg_resources
pkg_resources.require("sqlparse==0.3.0")
import sqlparse


class Parser:
    
    def __init__(self, raw_query=""):
        self.raw_query = raw_query


    def format_query(self, raw_query):
        """
        Format a query using sqlparse.
        Args:
            param (str): A string of raw query.
        Returns:
            str: A string of formatted query.
        """
        raw_query = self.raw_query.replace("\"", '')
        formatted_query = sqlparse.format(raw_query, \
                                        reindent=True, \
                                        keyword_case='upper', \
                                        strip_comments=True)

        return formatted_query


    def parse_cte(self, query):
        """
        Parse the CTE's.
        Args:
            param (str): A string of query containing CTE's.
        Returns:
            dict: A dict of CTE's and main query, with keys being CTE aliases or "main".  
        """
        cte = re.compile(r"(WITH)*(.*AS\s*\(SELECT)")
        pos_list = []
        for pos in cte.finditer(query):
            pos_list.append(pos.start())
        
        cte_main = re.compile(r"(SELECT)")
        pos_list_main = []
        for pos in cte_main.finditer(query):
            pos_list_main.append(pos.start())
           
        if pos_list != []:
            cte_dict = {}
            for index, pos in enumerate(pos_list):
                if index < len(pos_list)-1:
                    cte_query = query[pos:pos_list[index+1]]
                else:
                    cte_query = query[pos:pos_list_main[-1]]

                cte_query = cte_query.rstrip('\n,')
                cte_query = re.sub(r"\)$", "", cte_query)

                cte_name = re.findall(r"(WITH)*(.*)AS", cte_query)[0][1].strip(' ')    
                cte_removed = re.compile(r"\(SELECT")
                pos_list_removed = []

                for pos in cte_removed.finditer(cte_query):
                    pos_list_removed.append(pos.start())
                    
                cte_dict[cte_name] = cte_query[pos_list_removed[0]+1:]

            cte_dict['main_query'] = query[pos_list_main[-1]:]
            
        else:
            cte_dict = {}
            cte_dict['main_query'] = query

        return cte_dict    


    def _cleanup(self, cte_dict):

        for cte_name, cte in cte_dict.items():
            if cte_name != 'main':
                remove_head = re.search(r"\(", cte).start() + 1
                cte_dict[cte_name] = cte.replace(cte[:remove_head], '')

        return cte_dict  
        

    def get_table_names(self, line_query):
        """
        Get all tables names mapping from a SQL query. 
        Args:
            param (list): The flist of a query that is split by \n.
        Returns:
            dict: A dictionary with table names mapping. If a table is being aliased, returns key being the alias and value being the table/subquery name/alias. Otherwise, key and value is the same.
        """
        table_name_mapping = dict()

        for line in line_query:
            
            table_line = re.findall(r"(FROM|JOIN).(\w+.*)", line)

            if table_line != []:
                table_name_line = table_line[0][1].split(' ')
                
                if len(table_name_line) == 1:
                    table_name_mapping[table_name_line[0].rstrip(')|,')] = table_name_line[0].rstrip(')|,')

                if len(table_name_line) == 2:
                    table_name_mapping[table_name_line[-1].rstrip(')|,')] = table_name_line[0].rstrip(')|,')

                elif len(table_name_line) > 2 and table_name_line[1] == 'AS':
                    table_name_mapping[table_name_line[2].rstrip(')|,')] = table_name_line[0].rstrip(')|,')
                
                elif len(table_name_line) > 2 and table_name_line[1] != 'AS':
                    table_name_mapping[table_name_line[1].rstrip(')|,')] = table_name_line[0].rstrip(')|,')

        return table_name_mapping


    def _get_all_variables(self, query):
        """
        Get all variables including: table names, aliases, column names and aliases, and all other non-sql reserved words.
        Args:
            param (string): A string of any type of complete query; allows only complete query but can nest with CTE's and/or subqueries.
        Returns:
            list: A list of all variables within the query.
        """
        all_variables = []
        for e in query.split('\n'):

            if sum(list(map(lambda x: '*' in x, re.findall(r"([a-z].*[*])", e)))):
                
                if re.findall(r"(SELECT)", e) == [] and re.findall(r"([A-Z]+)", e) != []:
                    variable = [x.strip(' ') for x in re.findall(r"[a-z0-9_\s.]+", e)] 
                else:
                    variable = re.findall(r"([a-z].*[*])", e)
                
            elif sum(list(map(lambda x: '*' in x, re.findall(r"([*])", e)))):
            
                if re.findall(r"(SELECT)", e) == [] and re.findall(r"([A-Z]+)", e) != []:
                    variable = [x.strip(' ') for x in re.findall(r"[a-z0-9_\s.]+", e)] 
                    
                else:
                    variable = re.findall(r"([*])", e)
                
            else:
                variable = [x.strip(' ') for x in re.findall(r"[a-z0-9_\s.]+", e)]

            all_variables.extend(variable)

        return all_variables


    def _get_queried_columns(self, table_names, meta_cols):
        """
        Get all columns by looking up referenced table names in the metacolumn file.
        Args:
            param1 (dict): A dictionary of mapping of table/subquery referenced in query and their aliases.
            param2 (dict): A dictionary of metadata columns from Glue.
        Returns:
            list: A list of {key, value} pairs, each pair reflecting the table name and all columns under the table, from Glue metastore.
        """
        queried_cols = []

        for _,table_name in table_names.items():

            if len(table_name.split('.')) == 2:
                queried_cols.append({table_name: set(meta_cols[meta_cols['db_table'] == table_name]['all_columns'])})

        return queried_cols
            

    def _map_db_columns(self, var_list, queried_cols, table_alias_mapping):
        """
        Map database columns.
        Args:
            var_list (list): The list of all variables (non-sql reserved words) in query.
            queried_cols (list): The list of all currently existing columns in Glue, under the table that was being queried. 
            table_alias_mapping (dict): The mapping of table and their (if) alias.
        Returns:
            list: A list of unique db.table.column that was being scanned by the query.
        """
        original_columns_list = []

        for var in set(var_list):

            var = var.strip(' ')

            if var in table_alias_mapping.keys():
                pass
            
            else:
                var_split = var.split('.')
            
                if len(var_split) == 1:
                    if var_split[0] == '*':
                        for db_table in queried_cols:
                            for k,v in db_table.items():
                                for col in v:
                                    original_columns_list.append("{}.{}".format(k, col))
                        
                    else:
                        for db_table in queried_cols:
                            for k,v in db_table.items():
                                if var in v:
                                    original_columns_list.append("{}.{}".format(k, var))

                elif len(var_split) == 2:

                    if var_split[0] in table_alias_mapping.keys():
                        db_table = table_alias_mapping[var_split[0]]
                    
                        for db_table_col in queried_cols:
                            for k,v in db_table_col.items():

                                if k == db_table and var_split[1] in v:
                                    original_columns_list.append("{}.{}".format(k, var_split[1]))
                                elif k == db_table and var_split[1] == '*':
                                    for col in v:
                                        original_columns_list.append("{}.{}".format(k, col))     

        return list(set(original_columns_list))
            

    def _get_all_scanned_cols(self, cte_queries, meta_cols):
        """
        Get all scanned original columns.
        Args:
            param1 (dict): A dictionary of CTE's name:query pair.
            param2 (dict): A dictionary of metadata columns from Glue.
        Returns:
            list: A list of all scanned columns with db and table names.
        """
        all_columns_scanned = []

        for _,cte_query in cte_queries.items():

            table_alias_mapping = self.get_table_names(cte_query.split('\n'))
            variables = self._get_all_variables(cte_query)
            queried_columns = self._get_queried_columns(table_alias_mapping, meta_cols)
            if variables == []:
                original_columns_list = []
                for table in queried_columns:
                    for k,v in table.items():
                        for t in v:
                            original_columns_list.append("{}.{}".format(k,t))
            else:
                original_columns_list = self._map_db_columns(variables, queried_columns, table_alias_mapping)
            all_columns_scanned.extend(list(set(original_columns_list)))

        return all_columns_scanned


    def match_queried_fields(self, query, db_fields, **kwargs):
        """
        Match the query column with those registered on metastore.
        Args:
            query (string): the raw query.
            db_fields (spark dataframe): dataframe containing column names.
            **kargs: other metadata around query execution that needs to be populated to payload.

        Return:
            column_payload (json): the queried columns, table and db.
        """
        logging.info("Reading and formatting query...")
        
        formatted_query = self.format_query(query)
        cte_queries = self.parse_cte(formatted_query)

        logging.info("Mapping and retrieving columns from query...")
        all_columns_scanned = self._get_all_scanned_cols(cte_queries, db_fields)
        logging.info("All columns scanned in the query: {}.".format(all_columns_scanned))

        column_payload = []
        for column in all_columns_scanned:
            try:
                col_split = column.split('.')
                db, table, col = col_split[0], col_split[1], col_split[2]
                row_payload = dict()
                row_payload["database_name"] = db
                row_payload["table_name"] = table 
                row_payload["column_name"] = col
                for arg, value in kwargs.items():
                    row_payload[arg] = value    

                column_payload.append(row_payload)  
            except:
                pass

        return column_payload
