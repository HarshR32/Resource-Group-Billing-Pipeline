from pyspark.sql.functions import col
from pyspark.sql.functions import sum as _sum

def calculate_allocation(spark, df, agg_df):
    
    joined_df= df.join(agg_df, df['ResourceGroup']==agg_df['resourceGroupName'], how='left')
    
    return joined_df.withColumn('Allocation', col('TotalBilling') * col('Allocation %') / 100).drop('resourceGroupName')

def group_and_agg(df, group_by, agg_on, agg_alias):
    
    return df.groupby(group_by).agg(_sum(agg_on).alias(agg_alias))