from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

spark = SparkSession.builder.appName("AmountSpent").getOrCreate()

schema = StructType([ \
                     StructField("custID", IntegerType(), True), \
                     StructField("Item", IntegerType(), True), \
                     StructField("amount_spent", FloatType(), True)])

# // Read the file as dataframe
df = spark.read.schema(schema).csv("file:///SparkCourse/customer-orders.csv")
df.printSchema()

#Select only custID and amount_spent
spent = df.select("custID","amount_spent")

#Groupby and add amount spent for each customer
result = spent.groupBy("custID").agg(func.round(func.sum("amount_spent"),2).alias("Total_Amount_Spent"))
final_result = result.sort("Total_Amount_Spent")
final_result.show(result.count())

spark.stop()