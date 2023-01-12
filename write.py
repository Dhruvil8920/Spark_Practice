from pyspark.sql import *
from pyspark.sql.types import StructType, StructField, MapType, StringType
from datetime import date


spark = SparkSession.builder.getOrCreate()



data = [("Alice", 0, "2000-12-10"), ("Bob", 1, "1996-04-21"), ("Charlie", 2, "1990-08-15"), ("Fred", 3, "2004-03-27")]
st = ["name", "id", "dob"]
ef = spark.createDataFrame(data=data,  schema=st)
# ef.show()

df8 = ef.select("dob")
col = df8.select("dob")
col1 = col.collect()

# rdd12 = df8.rdd
print(col1)
a=[]

# def cage(y,m,d):
#     today = date.today()
#     age = today.year - y.year - ((today.month, today.day) < (dob.month, dob.day))
#     return age
#
#
# for i in col:
#
#     age = cage(i)
#     a.append(age)