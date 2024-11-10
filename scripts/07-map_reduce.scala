""" Importa funções """

import org.apache.spark.sql.functions._
import spark.implicits._


""" Carrega os dados """

val dados = spark.read.option("header","true").option("inferSchema","true").option("delimiter",";").format("csv").load("data/bank-additional-full.csv")


""" Função map e reduce """

dados.map(row => "Job: " + row.getAs[String]("job")).show(2)

dados.map (linha => linha.getAs[String]("poutcome").toUpperCase()).show(5)

dados.map(row => row.getAs[String]("poutcome").size).reduce(_+_)
