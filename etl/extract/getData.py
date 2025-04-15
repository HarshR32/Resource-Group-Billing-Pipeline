def get_data(spark, path, schema: str= None):
    try:
        if schema:
            df= spark.read.csv(
                path,
                header= True,
                schema= schema
            )
        else:
            df= spark.read.option('inferSchema', 'true').csv(
                path,
                header= True
            )
        
    except Exception as e:
        print(f'Error: file load not successful. Creating new dataframe.')
        df= spark.createDataFrame([], schema= schema)

    return df

def get_stream(spark, path, schema):
    return spark.readStream \
        .format('csv') \
        .option('header', True) \
        .schema(schema) \
        .load(path)
