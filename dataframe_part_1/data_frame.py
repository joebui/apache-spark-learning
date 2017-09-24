import findspark
findspark.init('/home/dienbui/spark-2.2.0-bin-hadoop2.7')
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StringType, IntegerType, StructType
from pyspark.sql.functions import countDistinct, avg, stddev, format_number

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

# Basic operations
df = spark.read.csv('appl_stock.csv', inferSchema=True, header=True)
df.filter('Close < 500').show()
df.filter('Close < 500').select('Open').show()
df.filter(df['Close'] < 500).select('Volume').show()
df.filter((df['Close'] < 200) & (df['Open'] > 200)).show()
result = df.filter(df['Low'] == 197.16).collect()
row = result[0]
row.asDict()

# GroupBy and aggregate
df.groupBy('Company').mean().show()
# +-------+-----------------+
# |Company|       avg(Sales)|
# +-------+-----------------+
# |   APPL|            370.0|
# |   GOOG|            220.0|
# |     FB|            610.0|
# |   MSFT|322.3333333333333|
# +-------+-----------------+

df.groupBy('Company').sum().show()
df.groupBy('Company').min().show()
df.groupBy('Company').max().show()
df.groupBy('Company').count().show()

df.agg({'Sales': 'sum'}).show()
# +----------+
# |sum(Sales)|
# +----------+
# |    4327.0|
# +----------+

group_data = df.groupBy('Company')
group_data.agg({'Sales': 'max'}).show()
# +-------+----------+
# |Company|max(Sales)|
# +-------+----------+
# |   APPL|     750.0|
# |   GOOG|     340.0|
# |     FB|     870.0|
# |   MSFT|     600.0|
# +-------+----------+

df.select(countDistinct('Sales')).show()
# +---------------------+
# |count(DISTINCT Sales)|
# +---------------------+
# |                   11|
# +---------------------+

df.select(avg('Sales').alias('Average Sales')).show()
# +-----------------+
# |    Average Sales|
# +-----------------+
# |360.5833333333333|
# +-----------------+

df.select(stddev('Sales')).show()
# +------------------+
# |stddev_samp(Sales)|
# +------------------+
# |250.08742410799007|
# +------------------+


sales_std = df.select(stddev('Sales'))
sales_std.select(format_number('std', 2)).show()
# +---------------------+
# |format_number(std, 2)|
# +---------------------+
# |               250.09|
# +---------------------+

df.orderBy('Sales').show()
# +-------+-------+-----+
# |Company| Person|Sales|
# +-------+-------+-----+
# |   GOOG|Charlie|120.0|
# |   MSFT|    Amy|124.0|
# |   APPL|  Linda|130.0|
# |   GOOG|    Sam|200.0|
# |   MSFT|Vanessa|243.0|
# |   APPL|   John|250.0|
# |   GOOG|  Frank|340.0|
# |     FB|  Sarah|350.0|
# |   APPL|  Chris|350.0|
# |   MSFT|   Tina|600.0|
# |   APPL|   Mike|750.0|
# |     FB|   Carl|870.0|
# +-------+-------+-----+

df.orderBy(df['Sales'].desc()).show()
# +-------+-------+-----+
# |Company| Person|Sales|
# +-------+-------+-----+
# |     FB|   Carl|870.0|
# |   APPL|   Mike|750.0|
# |   MSFT|   Tina|600.0|
# |     FB|  Sarah|350.0|
# |   APPL|  Chris|350.0|
# |   GOOG|  Frank|340.0|
# |   APPL|   John|250.0|
# |   MSFT|Vanessa|243.0|
# |   GOOG|    Sam|200.0|
# |   APPL|  Linda|130.0|
# |   MSFT|    Amy|124.0|
# |   GOOG|Charlie|120.0|
# +-------+-------+-----+
