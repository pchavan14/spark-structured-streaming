# get the data from a kafka source and send it back to kafka
# read invoices from kafka topic , create a notification record and send the notification record to kafka topics
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, expr
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType, IntegerType, ArrayType

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("Kafka Sink Demo") \
        .master("local[3]") \
        .config("spark.streaming.stopGracefullyOnShutdown", "true") \
        .config("spark.jars.packages","org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1") \
        .getOrCreate()


    schema = StructType([
        StructField("InvoiceNumber", StringType()),
        StructField("CreatedTime", LongType()),
        StructField("StoreID", StringType()),
        StructField("PosID", StringType()),
        StructField("CashierID", StringType()),
        StructField("CustomerType", StringType()),
        StructField("CustomerCardNo", StringType()),
        StructField("TotalAmount", DoubleType()),
        StructField("NumberOfItems", IntegerType()),
        StructField("PaymentMethod", StringType()),
        StructField("CGST", DoubleType()),
        StructField("SGST", DoubleType()),
        StructField("CESS", DoubleType()),
        StructField("DeliveryType", StringType()),
        StructField("DeliveryAddress", StructType([
            StructField("AddressLine", StringType()),
            StructField("City", StringType()),
            StructField("State", StringType()),
            StructField("PinCode", StringType()),
            StructField("ContactNumber", StringType())
        ])),
        StructField("InvoiceLineItems", ArrayType(StructType([
            StructField("ItemCode", StringType()),
            StructField("ItemDescription", StringType()),
            StructField("ItemPrice", DoubleType()),
            StructField("ItemQty", IntegerType()),
            StructField("TotalValue", DoubleType())
        ]))),
    ])

    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "invoices") \
        .option("startingOffsets", "earliest") \
        .load()

    kafka_df.printSchema()
   
    value_df = kafka_df.select(col("key"),from_json(col("value").cast("string"), schema).alias("value"))

    notifications_df = value_df.select("value.InvoiceNumber","value.CustomerCardNo","value.TotalAmount").withColumn("EarnedLoyaltyPoints",col("TotalAmount") * 0.2)

    #convert the dataframe to key and value
    kafka_target_df = notifications_df.selectExpr("InvoiceNumber as key",
                                                 """to_json(named_struct(
                                                 'CustomerCardNo', CustomerCardNo,
                                                 'TotalAmount', TotalAmount,
                                                 'EarnedLoyaltyPoints', TotalAmount * 0.2)) as value""")

    #we got an error because kafka accepts key and value pair , but our dataframe has differnet values, if we want to send dataframe to kafka it must have two columns key and value
    notifications_writer_query = kafka_target_df.writeStream.format("kafka").option("kafka.bootstrap.servers", "localhost:9092").option("topic","notifications").outputMode("append").option("checkpointLocation","chk-point_dir").start()

    notifications_writer_query.awaitTermination()
