#!/usr/bin/python3

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

print(df['age'])
print(type(df['age']))
print(df.select('age'))
df.select('age').show()
df.head(2)
df.select(['age', 'name']).show()

df.withColumn(('newage'), df['age']).show()
df.withColumn('double_age', df['age'] * 2).show()
df.createOrReplaceTempView('people')
results = spark.sql('SELECT * FROM people').show()
new_result = spark.sql('SELECT * FROM people where age = 30').show()

# New part
df = spark.read.csv('appl_stock.csv', inferSchema=True, header=True)
df.filter('Close < 500').show()
df.filter('Close < 500').select('Open').show()
df.filter(df['Close'] < 500).select('Volume').show()
df.filter((df['Close'] < 200) & (df['Open'] > 200)).show()
result = df.filter(df['Low'] == 197.16).collect()
row = result[0]
row.asDict()
