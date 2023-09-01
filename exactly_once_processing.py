from pyspark.sql import SparkSession
from pyspark.sql.functions import expr , col , explode

#schema needs to be defined for streaming data
if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("File streaming demo") \
        .config("spark.streaming.stopGracefullyOnShutdown","true") \
        .config("spark.sql.streaming.schemaInference","true") \
        .getOrCreate()
    

    raw_df = spark.readStream.format("json").option("path","input").load()



    #by deafult schema infer is disabled for streaming sources

    exploded_df = raw_df.select(col("InvoiceNumber"), col("CreatedTime") , col("StoreID"), col("PosID"), 
                                col("CustomerType"), col("PaymentMethod"), col("DeliveryType"),
                                col("DeliveryAddress.City"), col("DeliveryAddress.State") , col("DeliveryAddress.PinCode") , 
                                explode(col("InvoiceLineItems")).alias("LineItem"))
    


    #create a writer query and write a  dataframe

    invoice_writer_query = exploded_df.writeStream \
                            .format("json") \
                            .option("path", "output") \
                            .option("checkpointLocation","chk-point-dir") \
                            .outputMode("append") \
                            .queryName("Invoice Writer") \
                            .trigger(processingTime = "1 minute") \
                            .start() 

    invoice_writer_query.awaitTermination()