""" Importa funções """

import org.apache.spark.sql.types.{StructField, StructType, StringType, LongType, DoubleType, IntegerType}
import org.apache.spark.sql.functions.{col, column, expr, upper, lower, desc, asc, split, udf}
import org.apache.spark.sql.Row


""" Carrega os dados """

val dados = spark.read.option("header","true").option("inferSchema","true").option("delimiter",";").format("csv").load("data/bank-additional-full.csv")


""" Filtrando e ordenando dados (consultas básicas) """

dados.select($"age", $"job").filter($"age" > 55).orderBy($"age".desc).show(2)
dados.select($"age", $"marital").filter($"marital" === "married").show(5)
dados.select($"age", $"marital").filter($"marital".equalTo("married")).show(5)

dados.select($"age", $"marital").where($"marital" =!= "married").show(5)
dados.select($"age", $"marital").where("marital <> 'married' ").show(5)

dados.select($"marital").distinct().show()

val filtro_idade = col("age") > 40
val filtro_civil = col("marital").contains("married")
dados.select($"age", $"marital", $"job").where(col("job").isin("unemployed", "retired")).where(filtro_civil.or(filtro_idade)).show(5)
dados.select($"age", $"marital", $"job").where(col("job").isin("unemployed", "retired")).where(filtro_civil.or(filtro_idade)).withColumn("filtro_civil", filtro_civil).show(5)


""" Convertendo dados """

val dados1 = dados.withColumn("idade_string", col("age").cast("string"))
dados1.select($"idade_string")


""" Trabalhando com funções """

dados.select(upper($"poutcome")).show(1)
dados.select(lower($"poutcome")).show(1)
