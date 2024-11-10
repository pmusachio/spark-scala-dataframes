""" Importa funções """

import org.apache.spark.sql.types.{StructField, StructType, StringType, LongType, DoubleType, IntegerType}
import org.apache.spark.sql.functions.{col, column, expr, upper, lower, desc, asc, split, udf}
import org.apache.spark.sql.Row


""" Criando um Dataframe na mão """

//val schema = df.schema
val schema = new StructType(Array(
new StructField("id", IntegerType, true),
new StructField("job", StringType, true)))

val newRows = Seq(
Row(30, "Cientista de dados"),
Row(20, "Dev Java"),
Row(10, null)
)

val parallelizedRows = spark.sparkContext.parallelize(newRows)
val dados_manual = spark.createDataFrame(parallelizedRows, schema)
dados_manual.show()


""" Trabalhando dados complexos """

val dados_complexos = dados_manual.select(struct("id", "job").alias("complexo"))
dados_complexos.select("complexo.job").show(5)

val dados_split = dados_manual.select(split(col("job"), " ").alias("split_column"))
dados_split.show()

dados_split.selectExpr("split_column[0]").show()
dados_split.select(size($"split_column")).show()
dados_split.select(array_contains($"split_column", "Dev")).show()
dados_split.select(explode($"split_column")).show()

val dados_map = dados_manual.select(map(col("id"), col("job")).alias("mapa"))
dados_map.show()

dados_map.selectExpr("mapa['30']").show()
dados_map.selectExpr("explode(mapa)").show()


""" User Defined Functions (UDF) """

def incrementaId (number:Integer):Integer = number + 1
adicionaIdade(10)

val incrementaIdUDF = udf(incrementaId(_:Integer):Integer)

dados_manual.select($"id", incrementaIdUDF(col("id"))).show(5)

spark.udf.register("incrementaId", incrementaId(_:Integer):Integer)

dados_manual.selectExpr("incrementaId(id)").show(5)
