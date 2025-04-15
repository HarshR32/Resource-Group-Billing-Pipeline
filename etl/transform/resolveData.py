from pyspark.sql.functions import col, explode, size, when, split, trim

def explode_and_resolve(stream):
    return stream \
        .withColumn('projectArray', split('Projects', ';')) \
        .withColumn('Allocation %', when(col('Allocation %') == 'Equal', 100.0 / size(col('projectArray'))).otherwise(col('Allocation %').cast('double'))) \
        .withColumn('Projects', explode('projectArray')) \
        .withColumn('Projects', trim('Projects')) \
        .drop('projectArray')