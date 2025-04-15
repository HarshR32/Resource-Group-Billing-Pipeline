from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import coalesce, col, row_number
from pyspark.sql.window import Window

def upsert_jc(df: DataFrame, updates_df: DataFrame, composite_key: list) -> DataFrame:
    '''upsert function by joining and coalescing columns'''
    # create alias for avoiding ambiguity
    df_alias= df.alias('main')
    updates_df_alias= updates_df.alias('updates')

    # join the df on composite key
    joined_df= df_alias.join(updates_df_alias, on= composite_key, how= 'full_outer')

    # get all the non key columns
    non_key_cols= [col for col in df.columns if col not in composite_key]

    # coalesce non key columns
    upsertColumns= [coalesce(f'updates.{c}', f'main.{c}').alias(c) for c in non_key_cols]

    # coalesce key columns fo insertion
    return joined_df.select(*[f'{c}' for c in composite_key], *upsertColumns)

def upsert_md(existing_df, new_df, partition_cols, order_col):
    '''upsert function using merge and deduplication'''
    # combine the two df
    combined_df= existing_df.union(new_df)

    # create a window spec with parameterised partition and order columns
    window_spec= Window.partitionBy(*partition_cols).orderBy(col(order_col).desc())

    # apply and return the latest records
    return combined_df.withColumn('row_number', row_number().over(window_spec))\
                    .filter('row_number = 1')\
                    .drop('row_number')