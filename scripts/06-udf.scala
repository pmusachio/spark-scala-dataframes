""" IMPORT FUNCTIONS """"

import org.apache.spark.sql.types.{StructField, StructType, StringType, LongType, DoubleType, IntegerType}
import org.apache.spark.sql.functions.{col, column, expr, upper, lower, desc, asc, split, udf}
import org.apache.spark.sql.Row


""" CREATE A DATAFRAME MANUALY """

// load data using schema
val schema = new StructType(Array(
new StructField("age", IntegerType, true),
new StructField("job", StringType, true)))

val newRows = Seq(
Row(30, "Cientista de dados"),
Row(20, "Dev Java"),
Row(10, null)
)

val parallelizedRows = spark.sparkContext.parallelize(newRows)
val dados_manual = spark.createDataFrame(parallelizedRows, schema)
dados_manual.show()


""" WORKING W/ COMPLEX DATA """

// adding id and job in a single struct type column
val dados_complexos = dados_manual.select(struct("id", "job").alias("complexo"))
dados_complexos.select("complexo.job").show(5)

// split information from the job column when it finds space and returns an array
val dados_split = dados_manual.select(split(col("job"), " ").alias("split_column"))
dados_split.show()

// referencing array by position
dados_split.selectExpr("split_column[0]").show()

// count of positions in each array
dados_split.select(size($"split_column")).show()

// array_contains can check if a given element exists in the array
dados_split.select(array_contains($"split_column", "Dev")).show()

// "explore" can extract each element on a new line
dados_split.select(explode($"split_column")).show()

// "key-value" can work with MAP complex type
val dados_map = dados_manual.select(map(col("id"), col("job")).alias("mapa"))
dados_map.show()

// uses for MAP column
dados_map.selectExpr("mapa['30']").show()

dados_map.selectExpr("explode(mapa)").show()


""" User Defined Functions """

// creating a function
def incrementaId (number:Integer):Integer = number + 1
adicionaIdade(10)

// register function to be applied on Dataframes
val incrementaIdUDF = udf(incrementaId(_:Integer):Integer)

// using function
dados_manual.select($"id", incrementaIdUDF(col("id"))).show(5)

// registering function in Spark SQL
spark.udf.register("incrementaId", incrementaId(_:Integer):Integer)

// ready-to-use in SQL Context
dados_manual.selectExpr("incrementaId(id)").show(5)
