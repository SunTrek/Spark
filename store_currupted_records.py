

corrupted_data_df3 = spark.read.format('csv')\
    .option("header","true")\
        .option("inferSchema","true") \
            .schema(emp_schema)\
                .option("badRecordsPath","/FileStore/tables/bad_records")\
                .load("/FileStore/tables/employeed.csv")


corrupted_data_df3.show(truncate = False)
