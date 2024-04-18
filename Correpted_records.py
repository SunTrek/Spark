

corrupted_data_df = spark.read.format('csv')\
    .option("header","true")\
        .option("inferSchema","true") \
            .option("mode","dropmalformed") \
                .load("/FileStore/tables/employeed.csv")


corrupted_data_df.show()

+---+------+---+------+------------+--------+
| id|  name|age|salary|     address| nominee|
+---+------+---+------+------------+--------+
|  1|Manish| 26| 75000|       bihar|nominee1|
|  2|Nikita| 23|100000|uttarpradesh|nominee2|
|  5|Vikash| 31|300000|        null|nominee5|
+---+------+---+------+------------+--------+

==

from pyspark.sql.types import StructField, StructType, StringType, IntegerType

Storing Corrupted Records in Column:
emp_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("Name", StringType(), True),
    StructField("Age", IntegerType(), True),
    StructField("Salary", IntegerType(), True),
    StructField("Address", StringType(), True),
    StructField("Nominee", StringType(), True),
    StructField("Corrupted_record", StringType(), True)
])

corrupted_data_df2 = spark.read.format('csv')\
    .option("header","true")\
        .option("inferSchema","true") \
            .schema(emp_schema)\
            .option("mode","permissive") \
                .load("/FileStore/tables/employeed.csv")


corrupted_data_df2.show(truncate = False)

+---+--------+---+------+------------+--------+----------------+
|id |Name    |Age|Salary|Address     |Nominee |Corrupted_record|
+---+--------+---+------+------------+--------+----------------+
|1  |Manish  |26 |75000 |bihar       |nominee1|null            |
|2  |Nikita  |23 |100000|uttarpradesh|nominee2|null            |
|3  |Pritam  |22 |150000|Bangalore   |India   |nominee3        |
|4  |Prantosh|17 |200000|Kolkata     |India   |nominee4        |
|5  |Vikash  |31 |300000|null        |nominee5|null            |
+---+--------+---+------+------------+--------+----------------+
