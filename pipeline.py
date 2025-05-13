import os

from dotenv import load_dotenv

load_dotenv()

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import sum as _sum

from etl.extract.getData import get_data, get_stream
from etl.load.writeOut import write_batch, write_stream
from etl.transform.calAggregrates import calculate_allocation, group_and_agg
from etl.transform.upsertDataFrame import upsert_md, upsert_jc
from etl.transform.dataExtraction import extract_columns, extract_date
from etl.transform.resolveData import explode_and_resolve

from config.data import data, parent_path
from config.local import window, selected_columns, regex_pattern, file_schema

def process_project_allocation_stream(batch_df, batch_id):

    project_allocation_df= get_data(spark, data['intermediate']['project-allocations'], batch_df.schema)

    project_allocation_df= upsert_md(project_allocation_df, batch_df, window['project-allocations']['partition_by_cols'], window['project-allocations']['order_by_col'])

    write_batch(project_allocation_df, data['intermediate']['project-allocations'])

def process_billing_report_stream(batch_df, batch_id):
    
    key= 'resourceGroupName'
    group_by_spec= { 'group_by': key, 'agg_on': 'costInBillingCurrency', 'agg_alias': 'TotalBilling'}

    ba_df= get_data(spark, data['intermediate']['billing-analysis'], file_schema['billing-analysis'])

    resource_groups= [row[key] for row in batch_df.select(key).distinct().collect()]

    for group in resource_groups:
        updates_df= batch_df.filter(
            batch_df[key]==group
        )
        group_path= os.path.join(data['intermediate']['billing-reports'], f'Billing-Report-({group})')

        records_df= get_data(spark, group_path, batch_df.schema)
        
        group_updated_df= upsert_md(records_df, updates_df, window['billing-reports']['partition_by_cols'], window['billing-reports']['order_by_col'])
        ba_updates_df = group_and_agg(group_updated_df, *group_by_spec.values())
        ba_df= upsert_jc(ba_df, ba_updates_df, [key])

        write_batch(group_updated_df, group_path)

    write_batch(ba_df, data['intermediate']['billing-analysis'])

def run_pipeline(stream_duration= int(os.getenv('PIPELINEDURATION'))):
    global spark
    spark= SparkSession.builder.appName(os.getenv('APPNAME')).master(os.getenv('MASTER')).getOrCreate()

    # main billing reports stream
    br_stream= get_stream(spark, data['raw-extracted']['billing-reports'], file_schema['billing-reports'])
    extracted_br_stream= extract_columns(br_stream, selected_columns)
    final_br_stream= extract_date(extracted_br_stream, regex_pattern['billing-reports'])

    # main project allocations stream
    pa_stream= get_stream(spark, data['raw-extracted']['project-allocations'], file_schema['project-allocations'])
    pa_stream_with_date= extract_date(pa_stream, regex_pattern['project-allocations'])
    final_pa_stream= explode_and_resolve(pa_stream_with_date)

    br_query= write_stream(final_br_stream, process_billing_report_stream, data['checkpoint']['billing-reports']).start()

    pa_query= write_stream(final_pa_stream, process_project_allocation_stream, data['checkpoint']['project-allocations']).start()

    # await termination
    br_query.awaitTermination(stream_duration)
    pa_query.awaitTermination(stream_duration)

    # stop the query
    br_query.stop()
    pa_query.stop()

    # using the results of the two streams get the allocation value of each project 
    project_allocation_df= get_data(spark, data['intermediate']['project-allocations'])

    billing_analysis_df= get_data(spark, data['intermediate']['billing-analysis'])

    df_wth_allocation= calculate_allocation(spark, project_allocation_df, billing_analysis_df)

    write_batch(df_wth_allocation, data['processed'])

    spark.stop()

if __name__== '__main__':
    stream_run_duration= 30
    run_pipeline(stream_run_duration)