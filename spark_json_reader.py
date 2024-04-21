##Line Delimited Json
json_reader_df = spark.read.format('json')\
    .option("header","true")\
        .option("inferSchema","true") \
                .option("mode","permissive")\
                .load("/FileStore/tables/one_liner_json.json")


json_reader_df.show()


## Extra field in Line Delimited JSON
json_reader_df = spark.read.format('json')\
    .option("header","true")\
        .option("inferSchema","true") \
                .option("mode","permissive")\
                .load("/FileStore/tables/one_liner_json_extra_field.json")


json_reader_df.show()

Input Data:
{"name":"Manish","age":20,"salary":20000},
{"name":"Nikita","age":25,"salary":21000},
{"name":"Pritam","age":16,"salary":22000},
{"name":"Prantosh","age":35,"salary":25000},
{"name":"Vikash","age":67,"salary":40000,"gender":"M"}


--> Output of Spark code:
+---+------+--------+------+
|age|gender|    name|salary|
+---+------+--------+------+
| 20|  null|  Manish| 20000|
| 25|  null|  Nikita| 21000|
| 16|  null|  Pritam| 22000|
| 35|  null|Prantosh| 25000|
| 67|     M|  Vikash| 40000|
+---+------+--------+------+




