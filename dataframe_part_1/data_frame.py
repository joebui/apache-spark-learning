import findspark
findspark.init('/home/dienbui/spark-2.2.0-bin-hadoop2.7')
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StringType, IntegerType, StructType

spark = SparkSession.builder.appName('Basics').getOrCreate()
df = spark.read.json('people.json')
df.show()
df.printSchema()
df.columns
df.describe()
df.describe().show()

data_schema = [StructField('age', IntegerType(), True),
               StructField('name', StringType(), True)]
final_struct = StructType(fields=data_schema)
df = spark.read.json('people.json', schema=final_struct)
df.printSchema()
