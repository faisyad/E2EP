from pyspark.sql import SparkSession, HiveContext

#enableHiveSupport() -> enables sparkSession to connect with Hive
spark = SparkSession.builder \
    .appName("HiveExample") \
    .config("spark.sql.catalogImplementation", "hive") \
    .config("hive.metastore.uris", "thrift://localhost:9083") \
    .enableHiveSupport() \
    .getOrCreate()
#---------------------------------------------------------------------------------------------------------------#
spark.sql("CREATE DATABASE IF NOT EXISTS entuen")
# createdatabase = '''
#     CREATE DATABASE IF NOT EXISTS entuen
#     '''
#---------------------------------------------------------------------------------------------------------------#
spark.sql("USE entuen")
# usedatabase = '''
#     USE entuen
#     '''
#---------------------------------------------------------------------------------------------------------------#

createtable = '''
    CREATE EXTERNAL TABLE IF NOT EXISTS greentaxi2021 (
        VendorID BIGINT,
        lpep_pickup_datetime BIGINT,
        lpep_dropoff_datetime BIGINT,
        store_and_fwd_flag STRING,
        RatecodeID DOUBLE,
        PULocationID BIGINT,
        DOLocationID BIGINT,
        passenger_count DOUBLE,
        trip_distance DOUBLE,
        fare_amount DOUBLE,
        extra DOUBLE,
        mta_tax DOUBLE,
        tip_amount DOUBLE,
        tolls_amount DOUBLE,
        ehail_fee DOUBLE,
        improvement_surcharge DOUBLE,
        total_amount DOUBLE,
        payment_type DOUBLE,
        trip_type DOUBLE,
        congestion_surcharge DOUBLE
    )
    STORED AS PARQUET
    LOCATION 'hdfs://localhost:9000/user/faisyadd/e2eproject/greentaxi2'
    TBLPROPERTIES ('skip.header.line.count'='1')
'''
#---------------------------------------------------------------------------------------------------------------#
#spark.read.parquet('hdfs://localhost:9000/user/hive/warehouse/green_taxi2023/*.parquet').show()
spark.sql(createtable)
# # Show Tables
# spark.sql("show tables").show()

# # Execute HiveQL query
# result = spark.sql("select * from greentaxi2021").show()