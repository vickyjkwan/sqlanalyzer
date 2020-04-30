import json
import boto3
import datetime
import logging
import re
import time
from pyspark.sql.functions import udf, array
from pyspark.sql.types import ArrayType, StringType
from column_usage_util import format_query, parse_cte, get_all_scanned_cols
from pyspark.sql import SQLContext


def partition_mapping(partition_keys):
    """
    Spark UDF to get partition keys from spark row.
    Args:
        partition_keys (string): a spark df row

    Return:
        list: partition key names
    """
    if partition_keys != '':
        return [x['Name'] for x in json.loads(partition_keys)]
    else:
        return 'No Partition'


def extension(arr):
    """
    Spark UDF to get append partition keys to all column names.
    Args:
        arr (list): a list of column names or partition keys.
    
    Return:
        list: all column names.
    """
    if arr[1] is not None:
        arr[0].extend(arr[1])
        return arr[0]
    else:
        return arr[0]


def get_partition_keys(spark):
    """
    Get partition keys for all db_tables that are partitioned. 
    Args:
        spark (object): Spark object

    Return:
        spark dataframe: the dataframe containing db and table names and their partition keys.
    """
    part_keys_df = spark.sql(""" SELECT 
                                CONCAT(databasename, '.', name) AS db_table, 
                                partitionkeys 
                                FROM dwh_utils.glue_tables 
                                WHERE partitionkeys IS NOT NULL 
                                AND dt = (SELECT MAX(dt) FROM dwh_utils.glue_tables) """)
    return part_keys_df


def get_db_fields(spark, s3, run_date):
    """
    Get all columns.
    Args:
        spark (object): spark object.
        s3 (object): s3 boto3 resource object.
        run_date (string): the date of job run.

    Return:
        db_df (dataframe): db, tables and columns.
    """
    sc = spark.sparkContext
    sqlContext = SQLContext(sc)
    obj_db = s3.Object('mapbox-emr', 'dwh/column_usage/athena_db_fields/db_fields_{}.json'.format(run_date))
    file_db = obj_db.get()['Body'].read().decode('utf-8')
    json_db = json.loads(file_db)
    db_df = sqlContext.createDataFrame(json_db)

    return db_df


def get_query(s3, run_date, work_group):
    """
    Read query logs for work_group from json.
    Args:
        s3 (object): s3 boto3 resource object.
        run_date (string): the date of job run.
        work_group (string): the workgroup that queries were tagged by.

    Return: 
        query_json (json): query and execution details. 
    """
    query_obj = s3.Object('mapbox-emr', 'dwh/column_usage/athena_queries/{}_query_logs_{}.json'.format(work_group, run_date))
    query_content = query_obj.get()['Body'].read().decode('utf-8')
    query_json = json.loads(query_content)

    return query_json


def match_queried_fields(query, db_fields, **kwargs):
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

    formatted_query = format_query(query)
    cte_queries = parse_cte(formatted_query)

    logging.info("Mapping and retrieving columns from query...")
    all_columns_scanned = get_all_scanned_cols(cte_queries, db_fields)
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


def stitch_partition_fields(part_keys_df, db_fields):
    """
    Merge partition keys with the original columns for database.tables.
    Args:
        part_keys_df (spark dataframe): partition keys for each db.table.
        db_fields (spark dataframe): columns for each db.table.

    Return:
        db_with_partitions_df (spark dataframe): extended columns with partition key names for each db.table.
    """
    part_mapping_udf = udf(lambda y: partition_mapping(y), ArrayType(StringType()))
    partition_df = part_keys_df.select('db_table', part_mapping_udf('PartitionKeys').alias('partition_keys'))

    all_db = db_fields.join(partition_df, "db_table", how='left')

    extension_udf = udf(lambda x: extension(x), ArrayType(StringType()))
    db_with_partitions_df = all_db.select('db_table', extension_udf(array('column_list', 'partition_keys')).alias('all_columns'))

    return db_with_partitions_df


def parsed_df(spark, run_date, work_group):
    
    s3 = boto3.resource('s3')

    logging.info('Getting all fields since last snapshot...')
    db_fields = get_db_fields(spark, s3, run_date)
    part_keys_df = get_partition_keys(spark)
    db_with_partitions_df = stitch_partition_fields(part_keys_df, db_fields)

    logging.info('Getting all queries since last snapshot...')
    query_logs = get_query(s3, run_date, work_group=work_group)

    fields_rows = []
    
    for query in query_logs:
        logging.info("Parsing query_id {}".format(query['athena_query_id']))
        
        if work_group == 'primary':
            query_user = 'no user info'
        elif work_group == 'mode':
            if re.findall(r"\n-- (.*\})", query['query']) != []:
                query_user = json.loads(re.findall(r"\n-- (.*\})", query['query'])[0])['user']
            else:
                query_user = 'no user info'

        try:
            fields_row = match_queried_fields(query['query'], db_with_partitions_df, athena_query_id=query['athena_query_id'], \
                            execution_timestamp=query['query_submission_timestamp'], \
                            work_group=query['work_group'], query_user=query_user)
            fields_rows.extend(fields_row)
            logging.info('successfully parsed query_id {}'.format(query['athena_query_id']))
            
        except:
            logging.info("Failed to parse {}".format(query['athena_query_id']))
        
    sc = spark.sparkContext
    fields_rdd = sc.parallelize(fields_rows)
    fields_df = spark.read.json(fields_rdd)

    return fields_df
