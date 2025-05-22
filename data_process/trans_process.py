# Thêm các thư viện cần thiết 
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, from_unixtime, to_date, hour
from pyspark.sql.types import StructField, StructType, LongType, StringType, BooleanType, LongType
from kafka import KafkaConsumer
import time
# Jars
jars = [
    "/app/jars/commons-pool2-2.12.1.jar",
    "/app/jars/kafka-clients-3.9.0.jar",
    "/app/jars/spark-sql-kafka-0-10_2.12-3.5.0.jar",
    "/app/jars/spark-streaming-kafka-0-10_2.12-3.5.0.jar",
    "/app/jars/spark-token-provider-kafka-0-10_2.12-3.5.0.jar",
    "/app/jars/hadoop-aws-3.3.4.jar",
    "/app/jars/aws-java-sdk-bundle-1.12.262.jar",
    "/app/jars/delta-storage-3.0.0.jar",
    "/app/jars/delta-spark_2.12-3.0.0.jar",
]
#Config
config = {
    "spark.executor.memory"     :"1G",
    "spark.executor.instances"  :"1",
    "spark.executor.cores"      :"2",
    "spark.cores.max"           :"2",
    
    "spark.sql.session.timeZone":"Asia/Ho_Chi_Minh",
    "spark.sql.caseSensitive"   :"true",
    "spark.sql.extensions"      :"io.delta.sql.DeltaSparkSessionExtension",
    "spark.sql.catalog.spark_catalog"       :"org.apache.spark.sql.delta.catalog.DeltaCatalog",

    "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
    "spark.hadoop.fs.s3a.access.key"         :"16YVDwc7jtGCcqv751eb",
    "spark.hadoop.fs.s3a.secret.key"         :"vizxkrGKnvAKGL8cj1eV4ybX4g6VGSzoO1DGfHRl",
    "spark.hadoop.fs.s3a.endpoint"           : "http://minio:9000",
    "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
    "spark.hadoop.fs.s3a.path.style.access"  : "true",
    "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
    "spark.hadoop.fs.s3a.attempts.maximum"   : "1",
    "spark.hadoop.fs.s3a.connection.establish.timeout": "5000",
    "spark.hadoop.fs.s3a.connection.timeout" : "10000",
    "spark.hadoop.fs.s3a.metrics.logger.level": "WARN",
    "spark.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
    "spark.hadoop.aws.java.v1.disableDeprecationAnnouncement": "true",
    "spark.hadoop.fs.s3a.path.style.access": "true",
    "spark.hadoop.fs.s3a.fast.upload": "true",
    "spark.hadoop.fs.s3a.multipart.size": "16M",
    
    "spark.sql.shuffle.partitions": "2",
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.adaptive.coalescePartitions.enabled": "true",
    "spark.delta.merge.repartitionBeforeWrite": "true",
    "spark.databricks.delta.optimizeWrite.enabled": "true",
    "spark.databricks.delta.autoCompact.enabled": "true",
    "spark.databricks.delta.autoCompact.minNumFiles": "100000",
}
# Schema
kline_schema = StructType([
    StructField("e", StringType()),  # Event type
    StructField("E", LongType()),    # Event time
    StructField("s", StringType()),  # Symbol
    StructField("k", StructType([
        StructField("t", LongType()),     # Kline start time
        StructField("T", LongType()),     # Kline close time
        StructField("s", StringType()),   # Symbol
        StructField("i", StringType()),   # Interval
        StructField("f", LongType()),     # First trade ID
        StructField("L", LongType()),     # Last trade ID
        StructField("o", StringType()),   # Open price
        StructField("c", StringType()),   # Close price
        StructField("h", StringType()),   # High price
        StructField("l", StringType()),   # Low price
        StructField("v", StringType()),   # Base asset volume
        StructField("n", LongType()),     # Number of trades
        StructField("x", BooleanType()),  # Is this kline closed?
        StructField("q", StringType()),   # Quote asset volume
        StructField("V", StringType()),   # Taker buy base asset volume
        StructField("Q", StringType()),   # Taker buy quote asset volume
        StructField("B", StringType())    # Ignore
    ]))
])

# Khởi tạo spark, logger
def init_logger_spark(appname, master, jars, config):
    builder = SparkSession.builder.appName(appname).master(master)
    
    # Thêm jars nếu có
    if jars:
        jars_str = ",".join(jars)
        builder = builder.config("spark.jars", jars_str)

    # Thêm spark nếu có
    if config:
        for key, value in config.items():
            builder = builder.config(key, value)
            
    # Tạo spark
    spark = builder.getOrCreate()

    # Trích xuất logger từ spark
    sc = spark.sparkContext
    sc.setLogLevel("INFO") 
    log4jLogger = sc._jvm.org.apache.log4j
    logger = log4jLogger.LogManager.getLogger(__name__)
    
    logger.info("Create Spark Session Successfully")
    return spark, logger

# hàm kiểm tra sự tồn tại của kafka topic
def check_kafka_topic(topic_name, logger):
    try:
        consumer = KafkaConsumer(bootstrap_servers="kafka:9092")
        topics = consumer.topics()
        if topic_name in topics:
            logger.info(f"Topic {topic_name} đang hoạt động")
            return True
        else:
            logger.error(f"Topic {topic_name} không tồn tại")
            return False
    except Exception as e:
        logger.error(f"Lỗi khi kết nối đến Kafka {e}")
        return False  

# Xử lý dữ liệu
def data_process(spark, logger, kl_schema):
    logger.info("Bắt đầu quá trình xử lý dữ liệu streaming")
    try:
        data= spark.readStream \
                    .format("kafka") \
                    .option("kafka.bootstrap.servers","kafka:9092") \
                    .option("subscribe", "binance_kline_data") \
                    .option("startingOffsets", "earliest") \
                    .option("failOnDataLoss", "false") \
                    .option("maxOffsetsPerTrigger", "100000") \
                    .load()
        
        parsed_df = data.select(from_json(col("value").cast("string"), kl_schema).alias("data")).select("data.*")

        # Làm phẳng dữ liệu
        logger.info("Làm phẳng dữ liệu ...")
        kline_data = parsed_df.select(
            col("e").alias("event_type"),
            col("E").alias("event_time"),
            col("s").alias("symbol"),
            col("k.t").alias("kline_start_time"),
            col("k.T").alias("kline_close_time"),
            col("k.s").alias("kline_symbol"),
            col("k.i").alias("interval"),
            col("k.f").alias("first_trade_id"),
            col("k.L").alias("last_trade_id"),
            col("k.o").alias("open_price"),
            col("k.c").alias("close_price"),
            col("k.h").alias("high_price"),
            col("k.l").alias("low_price"),
            col("k.v").alias("base_asset_volume"),
            col("k.n").alias("number_of_trades"),
            col("k.x").alias("is_kline_closed"),
            col("k.q").alias("quote_asset_volume"),
            col("k.V").alias("taker_buy_base_asset_volume"),
            col("k.Q").alias("taker_buy_quote_asset_volume"),
            col("k.B").alias("ignore_field")
        )

        # Điền các giá trị null
        logger.info("Điền các giá trị null ...")
        kline_data_filled = kline_data.fillna({
            "event_type": "unknown",
            "event_time": 0,
            "symbol": "unknown",
            "kline_start_time": 0,
            "kline_close_time": 0,
            "kline_symbol": "unknown",
            "interval": "unknown",
            "first_trade_id": -1,
            "last_trade_id": -1,
            "open_price": "0.0",
            "close_price": "0.0",
            "high_price": "0.0",
            "low_price": "0.0",
            "base_asset_volume": "0.0",
            "number_of_trades": 0,
            "is_kline_closed": False,
            "quote_asset_volume": "0.0",
            "taker_buy_base_asset_volume": "0.0",
            "taker_buy_quote_asset_volume": "0.0",
            "ignore_field": "0.0"
        })

        # Chuyển đồi kiểu dữ liệu
        logger.info("Chuyển đổi kiểu dữ liệu ...")
        kline_data_casted = kline_data_filled \
            .withColumn("event_time", from_unixtime(col("event_time")/1000).cast("timestamp")) \
            .withColumn("kline_start_time", from_unixtime(col("kline_start_time")/1000).cast("timestamp")) \
            .withColumn("kline_close_time", from_unixtime(col("kline_close_time")/1000).cast("timestamp")) \
            .withColumn("first_trade_id", col("first_trade_id").cast("long")) \
            .withColumn("last_trade_id", col("last_trade_id").cast("long")) \
            .withColumn("open_price", col("open_price").cast("double")) \
            .withColumn("close_price", col("close_price").cast("double")) \
            .withColumn("high_price", col("high_price").cast("double")) \
            .withColumn("low_price", col("low_price").cast("double")) \
            .withColumn("base_asset_volume", col("base_asset_volume").cast("double")) \
            .withColumn("number_of_trades", col("number_of_trades").cast("long")) \
            .withColumn("is_kline_closed", col("is_kline_closed").cast("boolean")) \
            .withColumn("quote_asset_volume", col("quote_asset_volume").cast("double")) \
            .withColumn("taker_buy_base_asset_volume", col("taker_buy_base_asset_volume").cast("double")) \
            .withColumn("taker_buy_quote_asset_volume", col("taker_buy_quote_asset_volume").cast("double")) \
            .withColumn("ignore_field", col("ignore_field").cast("double"))\
            .withColumn("date", to_date(col("event_time"))) \
            .withColumn("hour", hour(col("event_time")))
        
        # Loại bỏ duplicate:
        logger.info("Loại bỏ các giá trị null ...")
        kline_data_drop_dup = kline_data_casted.drop_duplicates(['event_time','symbol']).withWatermark("event_time", "1 minutes")
        
        # Thông báo xử lý thành công
        logger.info("Xử lý dữ liệu thành công ...")
        # Thêm dữ liệu
        logger.info("Thêm dữ liệu vào MiniO(S3) ...")
        query = kline_data_drop_dup.writeStream.outputMode("append")\
            .format("delta")\
            .option("header", "true")\
            .option("checkpointLocation", "s3a://binance-data/checkpoints/kline_checkpoints")\
            .option("path", "s3a://binance-data/silver_data")\
            .option("maxFilesPerTrigger", "4") \
            .option("mergeSchema", "true") \
            .partitionBy("date", "hour") \
            .trigger(processingTime="10 minutes") \
            .start()
        return query
    except Exception as e:
        logger.warn(f"Lỗi trong quá trình xử lý : {str(e)}")
        raise

if __name__ == "__main__":
        # Khởi tạo spark, và logger
        spark, logger = init_logger_spark("BinanceKlineProcessor","spark://spark-master:7077", jars, config)
        kafka_available = False
        while not kafka_available:
            kafka_available = check_kafka_topic("binance_kline_data", logger)
            time.sleep(2)
            if kafka_available:
                query = data_process(spark, logger, kline_schema)
                query.awaitTermination()
            