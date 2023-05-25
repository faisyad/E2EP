from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when

#SparkSession + config hive
spark = SparkSession.builder \
    .appName("HiveExample") \
    .config("spark.sql.catalogImplementation", "hive") \
    .config("hive.metastore.uris", "thrift://localhost:9083") \
    .enableHiveSupport() \
    .getOrCreate()
#---------------------------------------------------------------------------------------------------------------#

spark.sql("CREATE DATABASE IF NOT EXISTS dimtable")
#---------------------------------------------------------------------------------------------------------------#

# buat dim pertama, vendor id
df = spark.sql ("""select distinct vendorid from entuen.greentaxi2021""")
df = df.withColumn("vendorname", when(col("vendorid") == 1, lit("Creative Mobile Technology,LLC"))
                        .when(col("vendorid") == 2, lit("Verifone Inc")))

spark.sql("use dimtable")
df.write.mode("overwrite").saveAsTable("dimvendorid")
#---------------------------------------------------------------------------------------------------------------#

# buat dim kedua, store and fwd flag
df = spark.sql ("""select distinct store_and_fwd_flag from entuen.greentaxi2021""")
df = df.withColumn("flaginfo", when(col("store_and_fwd_flag") == "Y", lit("Store and forward trip"))
                        .when(col("store_and_fwd_flag") == "N", lit("Not Store and forward trip"))
                        .when(col("store_and_fwd_flag").isNull(), lit(None)))
df = df.filter(df.store_and_fwd_flag.isNotNull()).select("*")

spark.sql("use dimtable")
df.write.mode("overwrite").saveAsTable("dimsff")
#---------------------------------------------------------------------------------------------------------------#

# buat dim ketiga, rate code id
df = spark.sql ("""select distinct ratecodeid from entuen.greentaxi2021""")
df = df.withColumn("codename", when(col("ratecodeid") == 1.0, lit("Standard rate"))
                        .when(col("ratecodeid") == 2.0, lit("JFK"))
                        .when(col("ratecodeid") == 3.0, lit("Newark"))
                        .when(col("ratecodeid") == 4.0, lit("Nassau or Westchester"))
                        .when(col("ratecodeid") == 5.0, lit("Negotiated fare"))
                        .when(col("ratecodeid") == 6.0, lit("Group Ride"))
                        .when(col("ratecodeid").isNull(), lit(None)))
df = df.filter(df.codename.isNotNull()).select("*")

spark.sql("use dimtable")
df.write.mode("overwrite").saveAsTable("dimratecodeid")
#---------------------------------------------------------------------------------------------------------------#

# buat dim keempat, payment
df = spark.sql ("""select distinct payment_type from entuen.greentaxi2021""")
df = df.withColumn("paymentname", when(col("payment_type") == 1.0, lit("Credit-card"))
                        .when(col("payment_type") == 2.0, lit("Cash"))
                        .when(col("payment_type") == 3.0, lit("No Charge"))
                        .when(col("payment_type") == 4.0, lit("Dispute"))
                        .when(col("payment_type") == 5.0, lit("Unknown"))
                        .when(col("payment_type") == 6.0, lit("Voided Trip"))
                        .when(col("payment_type").isNull(), lit(None)))
df = df.filter(df.payment_type.isNotNull()).select("*")

spark.sql("use dimtable")
df.write.mode("overwrite").saveAsTable("dimpayment")
#---------------------------------------------------------------------------------------------------------------#

# buat dim kelima, trip
df = spark.sql ("""SELECT DISTINCT trip_type FROM entuen.greentaxi2021""")
df = df.withColumn("tripname", when(col("trip_type") == 1.0, lit("Street-hail")) \
                        .when(col("trip_type") == 2.0, lit("Dispatch")))
df = df.filter(df.trip_type.isNotNull()).select("*")

spark.sql("use dimtable")
df.write.mode("overwrite").saveAsTable("dimtrip")
#---------------------------------------------------------------------------------------------------------------#