""" Importa funções """

import org.apache.spark.sql.functions.{col, column, expr}


""" Carrega os dados """

val dados = spark.read.option("header","true").option("inferSchema","true").option("delimiter",";").format("csv").load("data/bank-additional-full.csv")


""" Variações para referenciar colunas """

dados.select("age").show(5)
dados.select($"age").show(5)
dados.select('age).show(5)
dados.select(dados.col("age")).show(5)
dados.select(col("age")).show(5)
dados.select(column("age")).show(5)
dados.select(expr("age")).show(5)


""" Criando, removendo e alterando colunas """

val dados1 = dados.withColumn("nova_coluna", lit(1))

val teste = expr("age < 40")
dados.select("age", "y").withColumn("teste", teste).show(5)

dados.select(expr("age as idade")).show(5)
dados.select(col("age").alias("idade")).show(5)
dados.select($"age").withColumnRenamed("age", "idade").show(5)

val dados1 = dados.drop("age")
dados1.columns
