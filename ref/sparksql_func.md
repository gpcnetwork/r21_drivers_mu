pyspark cheat sheet
https://images.datacamp.com/image/upload/v1676302905/Marketing/Blog/PySpark_SQL_Cheat_Sheet.pdf 

spark.sql('''...query detail...''').toPandas()

qeury = spark.sql('''...query detail...''').cache()
query.createOrReplaceTempView("query_tbl")

spark.sql('''...query detail...''').printSchema()


Millenium Database Sub-Construct


|    |-- standard: struct (nullable = true)
|    |    |-- id: string (nullable = true)
|    |    |-- codingSystemId: string (nullable = true)
|    |    |-- primaryDisplay: string (nullable = true)
|    |-- standardCodings: array (nullable = true)
|    |    |-- element: struct (containsNull = true)
|    |    |    |-- id: string (nullable = true)
|    |    |    |-- codingSystemId: string (nullable = true)
|    |    |    |-- primaryDisplay: string (nullable = true)



|    |-- type: string (nullable = true)
|    |-- textValue: struct (nullable = true)
|    |    |-- value: string (nullable = true)
|    |-- numericValue: struct (nullable = true)
|    |    |-- value: string (nullable = true)
|    |    |-- modifier: string (nullable = true)
|    |-- unitOfMeasure: struct (nullable = true)
|    |    |-- standard: struct (nullable = true)
|    |    |    |-- id: string (nullable = true)
|    |    |    |-- codingSystemId: string (nullable = true)
|    |    |    |-- primaryDisplay: string (nullable = true)
|    |    |-- standardCodings: array (nullable = true)
|    |    |    |-- element: struct (containsNull = true)
|    |    |    |    |-- id: string (nullable = true)
|    |    |    |    |-- codingSystemId: string (nullable = true)
|    |    |    |    |-- primaryDisplay: string (nullable = true)
|    |-- codifiedValues: struct (nullable = true)
|    |    |-- values: array (nullable = true)
|    |    |    |-- element: struct (containsNull = true)
|    |    |    |    |-- value: struct (nullable = true)
|    |    |    |    |    |-- standard: struct (nullable = true)
|    |    |    |    |    |    |-- id: string (nullable = true)
|    |    |    |    |    |    |-- codingSystemId: string (nullable = true)
|    |    |    |    |    |    |-- primaryDisplay: string (nullable = true)
|    |    |    |    |    |-- standardCodings: array (nullable = true)
|    |    |    |    |    |    |-- element: struct (containsNull = true)
|    |    |    |    |    |    |    |-- id: string (nullable = true)
|    |    |    |    |    |    |    |-- codingSystemId: string (nullable = true)
|    |    |    |    |    |    |    |-- primaryDisplay: string (nullable = true)
|    |-- dateValue: struct (nullable = true)
|    |    |-- date: string (nullable = true)


|    |-- startDate: string (nullable = true)
|    |-- endDate: string (nullable = true)
