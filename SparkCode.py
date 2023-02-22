# Dependencies - Not dependecies needed but pyspark.
# I ran the file on the University Computer. To make it work, download the "dataset.txt" or rename the dataset file to it.
# Use the Python version lower than 3.7 , Choose java 8 when you set up your virtual machine and do "sudo update-alternatives --config java"
# How to run: "spark-submit --master local[*] <filename>.py" in terminal in a directory where the file is and where the "dataset.txt" is.
# There's an Info warning while running the file; However, it doesn't affect the solution.
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import*
from pyspark.sql.window import Window
dataset = 'dataset.txt'
spark = SparkSession.builder.master("local[1]").appName("PART-1").getOrCreate()
spark.sparkContext.setLogLevel('WARN')
df = spark.read.option("header", True).csv(dataset)
#Transform values to appropriate types
df = df
df = df.withColumn("UserID", df.UserID.cast(IntegerType()))
df = df.withColumn("Latitude", df.Latitude.cast(DoubleType()))
df = df.withColumn("Longitude", df.Longitude.cast(DoubleType()))
df = df.withColumn("Altitude", df.Altitude.cast(DoubleType()))
df = df.withColumn("Timestamp", df.Timestamp.cast(DoubleType()))
df = df.withColumn("Date", df.Date.cast(DateType()))
#1
print("\nTask 1:\n")
# If it's after 18:30, add 1 day to Date (after we added 5.5 hours it would be nextday)
df1 = df.withColumn("Date", when(df.Time >= "18:30:00", date_add(df.Date, 1)).otherwise(df.Date))
# Add 5.5 hours to Time
df1 = df1.withColumn("Time", date_format(df.Time + expr('INTERVAL 5 HOURS 30MINUTES'), 'HH:mm:ss'))
# Add 5.5 hours to the Timestamp
df1 = df1.withColumn("Timestamp", df.Timestamp + (5.5/24))
# Output the data
df1.show()
#2
print("\nTask 2:\n")
# Count how many times data was recorded for the same day for the same user
df2 = df1.groupBy("UserID", "Date").count()
# Select UserIDs and Days that have at least 2 data points recorded on a day
df2 = df2.where(col("count") >= 2)
# For each user, count on how many days at least 2 data points were recorded
# Sort by the number of count and UserID number
df2 = df2.groupBy("UserID").count().sort(desc("count"), desc("UserID"))
# Output top 10 rows
df2.show(10)
#3

print("\nTask 3:\n")
# Count how many times data was recorded for the same day for the same user
df3 = df1.groupBy("UserID", "Date").count()
# Select UserIDs and Days that have more than 150 data points recorded on a day
df3 = df3.where(col("count") > 150)
# For each user, count on how many days more than 150 data points were recorded
# Sort by the number of count and UserID number
df3 = df3.groupBy("UserID").count().sort(desc("count"), desc("UserID"))
# Output all the rows
df3.show(df3.count())
#4
print("\nTask 4:\n")
# Find the biggest Latitude for every User, the recent one in case of a tie
df4 =
df1.orderBy(desc("Date")).groupBy("UserID").agg(max("Latitude").alias("North_Latitude"))
# Create a new dataframe with Max Latitude for every User and corresponding Date
df4_5 = df1.groupBy("UserID", "Date").agg(max("Latitude").alias("Max_Latitude"))\
.withColumnRenamed("UserID", "User2")
# Join 2 data frames above to find out a Day on which the North_Latitude has been
achieved
df4 = df4.join(df4_5, (df4.UserID == df4_5.User2) & (df4.North_Latitude == df4_5.Max_Latitude))
# Drop duplicated UserID - (User2) and Duplicated North_Latitude - (Max_Latitude)
# Order dataframe by North_Latitude, Date and UserID
df4 = df4.drop("User2","Max_Latitude")\
.orderBy(col("North_Latitude").desc(), col("Date").desc(), col("UserID").desc())
# Output the top 10 rows
df4.show(10)
#5
print("\nTask 5:\n")
# Find the max and min altitude for every UserID
df5 = df1.groupBy("UserID").agg(max("Altitude").alias("Max_altitude"),min("Altitude").alias("Min_altitude"))
# Find for each user the difference between min and max altitudes achieved by them
df5 = df5.withColumn("Altitude_difference", df5.Max_altitude - df5.Min_altitude)
# Drop the unneccesary columns and sort by Altitude_difference, UserID
df5 = df5.drop("Max_altitude","Min_altitude")\
.orderBy(col("Altitude_difference").desc(), col("UserID").desc())
# Output the top 10 rows
df5.show(10)
#6
print("\nTask 6:\n")
# Partition the data for every UserID and order it by Timestamp (ASC)
partition = Window.partitionBy("UserID").orderBy("Timestamp")
# Create a lag on Altitude over the partition above
df6 = df1.withColumn("Lag_alti",lag("Altitude",1).over(partition))
# Create a new colum that is the difference between registered altitude and altitude registered at the next measure (from lag), drop the NULLS
df6 = df6.withColumn("High_difference", df6.Altitude - df6.Lag_alti).dropna()
# Drop all the values that are less or equal to 0. We don't count going down and 0s
are unneccesary.
df6 = df6.filter(df6.High_difference>0)
# Group the data by UserID and Data, sum up the distance climbed on each day for a
User.
df6 = df6.groupBy("UserID",
"Date").agg(sum("High_difference").alias("Total_height"))
# Task 6 Part 1
# Partition the data depending on a UserID and ordered by the Total_height and Date
partition2 = Window.partitionBy("UserID").orderBy(col("Total_height").desc(),col("Date").desc())
# Create a new column, indicating the number of a row (starts from 1 for each UserID)
df6_1 = df6.withColumn("RowNum", row_number().over(partition2))
# Remove all the rows that are not first (The Total_height isn't maximum for a user or was registered earlier in case of a tie)
# Order the data by the distance climbed and UserID
df6_1 = df6_1.filter(df6_1.RowNum==1).drop("RowNum")\
.orderBy(col("Total_height").desc(), col("UserID").desc())
# Output all the rows
df6_1.show(df6_1.count())
# Task 6 Part 2
# Sum up all the heights that Users climbed on all the days
df6_2 = df6.agg(sum("Total_height").alias("Total_Climbed"))
# Output the result (only 1 row)
df6_2.show()
