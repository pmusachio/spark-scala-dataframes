""" Importa funções """

import org.apache.spark.sql.types.{StructField, StructType, StringType, LongType, DoubleType, IntegerType}
import org.apache.spark.sql.functions.{col, column, expr, upper, lower, desc, asc, split, udf}
import org.apache.spark.sql.Row


""" Criando um Dataframe na mão """

//val schema = df.schema
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


""" Carrega os dados """

val dados = spark.read.option("header","true").option("inferSchema","true").option("delimiter",";").format("csv").load("data/bank-additional-full.csv")


""" Trabalhando com valores ausentes """

dados.select($"age", $"marital").na.drop("any")
dados.select($"age", $"marital").na.drop("all")
dados_manual.na.fill("Desconhecido").show()

val valores_para_preencher = Map("age" -> 0, "job" -> "Desconhecido")
dados_manual.na.fill(valores_para_preencher).show()


""" Substituindo valores """

dados_manual.na.replace("job", Map("Dev Java" -> "Desenvolvedor")).show()
