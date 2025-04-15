def write_batch(df, path, mode= 'append'):
    df.write.mode(mode).csv(path, header= True)

def write_stream(stream, function, checkpoint_path):
    return stream.writeStream \
        .foreachBatch(function) \
        .outputMode('append') \
        .option('checkpointLocation', checkpoint_path)