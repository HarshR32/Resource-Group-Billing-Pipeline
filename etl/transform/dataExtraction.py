from pyspark.sql.functions import input_file_name, to_date, regexp_extract

def extract_columns(stream, selected_columns):
    return stream.select(*selected_columns)

def extract_date(stream, pattern):
    return stream.withColumn('filename', input_file_name()) \
        .withColumn('latestDate', to_date(regexp_extract('filename', pattern, 1), "yyyyMMdd")) \
        .drop('filename')