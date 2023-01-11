import pyspark
from pyspark.sql import *

spark = SparkSession.builder.master("local[*]").appName("spa").getOrCreate()
# Read file
df = spark.read.option("header", True).csv("C:/Users/dhruv/Downloads/user.csv")

# Drop column
df.drop("emailid").show()

# Rename column
df.withColumnRenamed("user_id", " id").show()

# Select column
df.select("user_id").show()


print(df.count())

print(df.columns)   # Print every column name

# Dropping raw with null value

df.na.drop(how="any", thresh=2)     # Drooping raw who don't have at-least "2" nonnull values

df.na.drop(how="any", subset="user_id")     # Dropping raw who has null value only in "selected" column

# Filling values

# df.na.fill("Hello")     # Null value changes to "Hello"
df.na.fill("Hello", ["location "])    # Null value changes to "Hello"

# Filter value

df.filter("user_id>105").show()

df.filter((df["user_id"]>105) & (df["user_id"]>106)).show()     # Two condition

df.filter("user_id>105").select(["location ", "nativelanguage"]).show()     # Only show selected columns

# Aggregate func

df.agg({"user_id" : "sum"}).show()      # Sum of user_id

# Creating dataframe

data = [("Alice", 0), ("Bob", 1), ("Charlie", 2), ("Fred", 3)]

st = ["name", "id"]

ef = spark.createDataFrame(data=data,  schema=st)

ef.show()

# Distinct

ef.distinct().show()

# Creating RDD

rdd1 = spark.sparkContext.parallelize([("party", 1), ("wen", 2)])

print(rdd1.collect())

print(type(rdd1))

# Reading textfile into rdd

qf1 = spark.sparkContext.textFile("C:/Users/dhruv/Downloads/user.csv")
print(qf1.collect())
print(type(qf1))

# Converting dataframe to rdd

rdd2 = ef.rdd
print(type(rdd2))

# Converting rdd to dataframe

sc = ["name", "id"]
df1 = rdd1.toDF(sc)
print(type(df1))

# Transformation

li = [1, 2, 3, 4, 5]
rdd3 = spark.sparkContext.parallelize(li)


rdd4 = rdd3.map(lambda x: (x**2))       # Map function
print(rdd4.collect())

rdd5 = rdd3.filter(lambda x: (x % 2 == 0))      # Filter function
print(rdd5.collect())

rdd6 = rdd3.flatMap(lambda x: (x, x**2, x*100))     # Flatmap
print(rdd6.collect())


# Map-partition

rdd7 = spark.sparkContext.parallelize(li, 2)
print(rdd7.glom().collect())

# Max - Min

print(rdd3.max())
print(rdd3.min())

# Counting in RDD

rdd8 = spark.sparkContext.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 1, 5])

print(rdd8.countByValue())        # OutPut --> {1: 2, 2: 1, 3: 1, 4: 1, 5: 2, 6: 1, 7: 1, 8: 1, 9: 1, 10: 1}

# Distinct

rdd9 = spark.sparkContext.parallelize(["a", "b", "c", "a", "a", "b", "d", "e"])

value = rdd9.countByValue()
print(value)        # OutPut --> {'a': 3, 'b': 2, 'c': 1, 'd': 1, 'e': 1}

# Top - Take

print(rdd8.top(3))      # Top 3 value print

print(rdd8.take(3))        # First 3 value print

# Indexvalue on partition

rdd10=spark.sparkContext.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9, 10],2)

def f(partitionIndex, iterator):
    yield (partitionIndex, sum(iterator))

rdd11 = rdd8.mapPartitionsWithIndex(f)
print(rdd10.glome().collect())

# Selecting random value from list

sample = rdd3.takeSample(False, 3)      # OutPut --> [3, 4, 2]
print(sample)
print(type(sample))


sample1 = rdd3.sample(True, 0.2)     # OutPut --> [5]
print(sample1.collect())

# Union

union = rdd3.union(rdd8)
print(union.collect())      # OutPut --> [1, 2, 3, 4, 5, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 1, 5, 7]


# Intersection

intersection = rdd3.intersection(rdd8)
print(intersection.collect())       # OutPut --> [1, 2, 3, 4, 5]


# Cache and persist

rdd8.cache()        # Store in memory

rdd8.unpersist()        # Remove from memory

rdd3.persist(pyspark.StorageLevel.MEMORY_ONLY)      # Store in memory

# Broadcast variable

brodrdd = spark.sparkContext.broadcast([1, 2, 3])
print(brodrdd.value)        # Output --> [1, 2, 3]

brodrdd.unpersist()
brodrdd.destroy()

# Accumulator

accmulator = spark.sparkContext.accumulator(0)
spark.sparkContext.parallelize(li).foreach(lambda x: accmulator.add(x))
print(accmulator.value)         # OutPut --> 15

accmulator = spark.sparkContext.accumulator(5)
spark.sparkContext.parallelize(li).foreach(lambda x: accmulator.add(x))
print(accmulator.value)         # OutPut --> 20


# New CSV

ndf = spark.read.csv("E:/Downloads/annual-enterprise-survey-2021-financial-year-provisional-csv.csv", inferSchema=True, header=True)
ndf.show()
print(ndf.schema)

# GroupBy

ndf1 = ndf.groupBy("Variable_category").count()
ndf1.show()

# Sort

ndf.sort("year")

# StructType and StructField

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

data = [(1, ("Maheer", "shatk"), 3000), (2, ("Wat", "Shatk"), 4000)]

structName = StructType([StructField('Firstname', StringType()), StructField("LastName", StringType())])

schema = StructType([StructField(name=' id', dataType=IntegerType()), StructField(name="name", dataType = structName),
                     StructField(name='salary', dataType=IntegerType())])

df2 = spark.createDataFrame(data, schema)
df2.show()

# Pivot Dataframe

List = [("Banana", 1000, "USA"), ("Carrots", 1500, "USA"), ("Beans", 1600, "USA"),
        ("Orange", 2000, "USA"), ("Orange", 2000, "USA"), ("Banana", 400, "China"),
        ("Carrots", 1200, "China"), ("Beans", 1500, "China"), ("Orange", 4000, "China"), ("Beans", 2000, "Mexico"),
        ("Banana", 2000, "Canada"), ("Carrots", 2000, "Canada")]

df = spark.createDataFrame(List, schema=('Fruits', "Price", "Country"))
df = df.groupby('Fruits', 'Country').sum("Price")
df.show()

List1 = [("Banana", 1000, "USA"), ("Carrots", 1500, "USA"), ("Beans", 1600, "USA"),
        ("Orange", 2000, "USA"), ("Orange", 2000, "USA"), ("Banana", 400, "China"),
        ("Carrots", 1200, "China"), ("Beans", 1500, "China"), ("Orange", 4000, "China"), ("Beans", 2000, "Mexico"),
        ("Banana", 2000, "Canada"), ("Carrots", 2000, "Canada")]

df = spark.createDataFrame(List1, schema=('Fruits', "Price", "Country"))

df = df.groupby('Fruits').pivot("Country").sum("Price")

df.show()

# Join Dataframe

df3 = spark.createDataFrame([("pubg", 1), ("valorant", 2), ("cod", 4)], schema="game string, id long")

df3.join(ef, df3.id == ef.id, "inner").select(df3["game"], ef["name"]).show()           # Inner Join

df3.join(ef, df3.id == ef.id, "right").select(df3["game"], ef["name"]).show()           # Right Join

df3.join(ef, df3.id == ef.id, "left").select(df3["game"], ef["name"]).show()            # Left Join

df3.join(ef, df3.id == ef.id, "full").select(df3["game"], ef["name"]).show()            # FUll Outer Join

# Union

df3.union(ef).orderBy("id").show()

# Map

conrdd = ef.rdd.map(lambda x: (x[0], x[1], x[1]*10))

consc = ["name", "id", "Hour"]

condf = conrdd.toDF(consc)

condf.show()

# Cache and persist

condf.persist(pyspark.StorageLevel.DISK_ONLY)

# ArrayType

from pyspark.sql.types import ArrayType, StringType, StructType, StructField

data2 = [("alpha", ["morning", "valo"]), ("beta", ["afternoon", "csgo"]), ("gama", ["evening", "ark"]),
         ("delta", ["night", "genshin"])]
sch = StructType([StructField("name", StringType()),
                 StructField("game", ArrayType(StringType()))])

df5 = spark.createDataFrame(data2, sch)
df5.show()
df5.select(df5.game[0]).show()
df5.printSchema()

# MapType

from pyspark.sql.types import MapType, StringType, StructType, StructField

data1 = [("alpha", {"morning": "valo"}), ("beta", {"afternoon": "csgo"}), ("gama", {"evening": "ark"}),
         ("delta", {"night": "genshin"})]
sch = StructType([StructField("name", StringType()),
                 StructField("game", MapType(StringType(), StringType()))])

df4 = spark.createDataFrame(data1, sch)
df4.show()