#ncat listener to open a socket and get the words

from pyspark.sql import SparkSession
from pyspark.sql.functions import expr

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("streaming word count") \
        .config("spark.streaming.stopGracefullyOnShutdown","true") \
        .config("spark.sql.shuffle.partitions",3) \
        .getOrCreate()

    #read a streaming source - input dataframe
    #transform - output dataframe
    #write to sink

    #readStream is similar infunctionality as createDataFrame , it is a streaming dataframe some standard operations are not allowed (cannot apply count)
    lines_df = spark.readStream.format("socket").option("host","localhost").option("port","9999").load()

    lines_df.printSchema()

    words_df = lines_df.select(expr("explode(split(value,' ')) as word"))

    #count aggregate is allowed on streaming dataframe
    counts_Df = words_df.groupBy("word").count()

    #write dataframe to sink  - data stream writer is similar to data frame writer
    word_count_query = counts_Df.writeStream.format("console").option("checkpointLocation","chk-point-dir").outputMode("complete").start() 
    #complete - it shows full output , the start() method starts the streaming

    #start- method starts a background job and return , so we must wait for the background job tom completed

    word_count_query.awaitTermination()
