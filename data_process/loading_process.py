from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, from_unixtime
from pyspark.sql.types import StructField, StructType, LongType, StringType, BooleanType, LongType
import time
jars = [
    "/opt/airflow/jars/commons-pool2-2.12.1.jar",
    
    "/opt/airflow/jars/hadoop-aws-3.3.4.jar",
    "/opt/airflow/jars/aws-java-sdk-bundle-1.12.262.jar",
    
    "/opt/airflow/jars/delta-storage-3.0.0.jar",
    "/opt/airflow/jars/delta-spark_2.12-3.0.0.jar",

    "/opt/airflow/jars/delta-spark_2.12-3.0.0.jar",
    "/opt/airflow/jars/spark-snowflake_2.12-3.0.0.jar",
    "/opt/airflow/jars/snowflake-jdbc-3.22.0.jar",
]
config = {
    "spark.executor.memory"     :"2G",
    "spark.executor.instances"  :"1",
    "spark.executor.cores"      :"2",
    "spark.cores.max"           :"2",
    "spark.sql.shuffle.partitions": "100",

    "spark.sql.session.timeZone":"Asia/Ho_Chi_Minh",
    "spark.sql.caseSensitive"   :"true",
    "spark.sql.extensions"      :"io.delta.sql.DeltaSparkSessionExtension",
    "spark.sql.catalog.spark_catalog"       :"org.apache.spark.sql.delta.catalog.DeltaCatalog",
    
    "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
    "spark.hadoop.fs.s3a.access.key"         :"TgOkWVD2osSrMpB8rKGX",
    "spark.hadoop.fs.s3a.secret.key"         : "GDTg3XG7TcI4wfCaQqGHM0GIV3Q2weWcavmbqRKc",
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
}
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

# Lấy ngày và giờ trước đó
def get_date_and_hour():
    from datetime import datetime, timedelta
    now = datetime.now()
    # Lùi về 1 giờ
    one_hour_ago = now - timedelta(hours=1)

    date_str = one_hour_ago.strftime("%Y-%m-%d")
    hour_str = one_hour_ago.strftime("%H")

    return date_str, hour_str

# Khởi tạo spark
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

# Hàm ghi DataFrame vào Snowflake
def write_to_snowflake(df, table_name, logger, sf_options):
    try:
        query = (df.write
            .format("snowflake")
            .options(**sf_options)
            .option("dbtable", table_name)
            .option("truncate_table", "off")
            .option("continue_on_error", "on")
            .mode("append")
            .save())
        
        logger.info(f"Started loading to Snowflake table {table_name}")
        return query
    except Exception as e:
        logger.error(f"Error writing to table {table_name}: {str(e)}")
        return None
    
# Hàm chính phân chia và loading dữ liệu
def spliting_loading_snowflake(spark, logger, sf_options, date,hour):
    logger.info("Starting streaming process to Snowflake...")
    try:
        # Read source data with streaming
        source_df = (spark.read
            .format('delta')
            .load("s3a://binance-data/silver_data")
            .where(f'date = "{date}" AND hour={hour}'))
        
        # Create state track stream
        state_track = (source_df.filter(col("is_kline_closed") == True)
            .select(
                "event_time",
                "symbol",
                "is_kline_closed"
            ))
        # Create trade tracks stream
        trade_tracks = source_df.select(
            "event_time",
            "symbol",
            "first_trade_id",
            "last_trade_id",
            "base_asset_volume",
            "number_of_trades",
            "quote_asset_volume",
            "taker_buy_base_asset_volume",
            "taker_buy_quote_asset_volume"
        )
        
        # Create data tracks stream
        data_tracks = (source_df.filter(col("is_kline_closed") == True)
            .select(
                "event_time",
                "symbol",
                "kline_start_time",
                "kline_close_time",
                "open_price",
                "close_price",
                "high_price",
                "low_price",
                "ignore_field"
            ))
        # Ghi dữ liệu với kiểm tra lỗi
        write_success = all([
            write_to_snowflake(state_track, "STATE_TRACKS", logger=logger,sf_options=sf_options),
            write_to_snowflake(trade_tracks, "TRADE_TRACKS", logger=logger,sf_options=sf_options),
            write_to_snowflake(data_tracks, "DATA_TRACKS", logger=logger,sf_options=sf_options)
        ])

        if write_success:
            logger.info("Tất cả dữ liệu đã được ghi thành công!")
        else:
            logger.info("Có lỗi xảy ra khi ghi một số bảng")
    except Exception as e:
        logger.error("Fatal error in streaming process", exc_info=True)
        raise

if __name__=='__main__':
    # Cấu hình Snowflake
    sf_options = {
        "sfUrl": "your-account.snowflakecomputing.com",
        "sfUser": "user_name",
        "sfPassword": "password",
        "sfDatabase": "CRYPTO",
        "sfSchema": "BINANCE",
        "sfWarehouse": "COMPUTE_WH",
        "sfRole": "ACCOUNTADMIN"
    }

    spark, logger = init_logger_spark(appname="Loading to Snowflake",
                                      master="spark://spark-master:7077",jars=jars, config=config)
    date, hour = get_date_and_hour()
    
    try:
        spliting_loading_snowflake(
            spark=spark,
            logger=logger,
            sf_options=sf_options,
            date=date,
            hour=hour
        )
    finally:
        # Cleanup
        spark.stop()
        logger.info("Spark session stopped")