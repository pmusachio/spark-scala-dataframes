""" Importa funções """

import org.apache.spark.sql.functions._


""" Carregando os dados """

val dados = spark.read.option("header","true").option("inferSchema","true").option("delimiter",";").format("csv").load("data/bank-additional-full.csv")


""" Trabalhando com partições """

dados.rdd.getNumPartitions
dados.rdd.partitions.size
dados.rdd.glom().map(_.length).collect()

val dados1 = dados.coalesce(1)
dados1.rdd.getNumPartitions
dados1.rdd.glom().map(_.length).collect()

val dados2 = dados.repartition(4)
dados2.rdd.getNumPartitions
dados2.rdd.glom().map(_.length).collect()

val dados3 = dados.repartition($"marital")
dados3.rdd.getNumPartitions
dados3.rdd.glom().map(_.length).collect()

val dados4 = dados3.coalesce(5)
