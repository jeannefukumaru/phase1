import pyspark
from delta import *

builder = pyspark.sql.SparkSession.builder.appName("MyApp") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

train_csv = spark.read.format('csv').load('data/creditdata/creditdata_train_v2.csv')
train_csv.write.format('delta').save('data/creditdata_train_delta')

test_csv = spark.read.format('csv').load('data/creditdata/creditdata_test_v2.csv')
test_csv.write.format('delta').save('data/creditdata_test_delta')
