""" IMPORT FUNCTIONS """"

import org.apache.spark.sql.types.{StructField, StructType, StringType, LongType, DoubleType, IntegerType}
import org.apache.spark.sql.functions.{col, column, expr, upper, lower, desc, asc, split, udf}
import org.apache.spark.sql.Row


""" CREATE A DATAFRAME MANUALY """

// load data using schema
val schema = new StructType(Array(
new StructField("age", IntegerType, true),
new StructField("job", StringType, true)))

// create rows
val newRows = Seq(
Row(30, "Cientista de dados"),
Row(20, "Dev Java"),
Row(10, null)
)

// create a RDD
val parallelizedRows = spark.sparkContext.parallelize(newRows)

// create dataframe from RDD
val dados_manual = spark.createDataFrame(parallelizedRows, schema)

// show dataframe info
dados_manual.show()


""" LOAD DATA """"

val dados = spark.read
                 .option("header","true")
                 .option("inferSchema","true")
                 .option("delimiter",";")
                 .format("csv")
                 .load("data/bank-additional-full.csv")


""" MISSING VALUES """

// drop the row if one value is null
dados.select($"age", $"marital").na.drop("any")

// drop the row if all values are null
dados.select($"age", $"marital").na.drop("all")

// fill
dados_manual.na.fill("Desconhecido")
            .show()

// specifying values for each column
val valores_para_preencher = Map("age" -> 0, "job" -> "Desconhecido")
dados_manual.na.fill(valores_para_preencher)
            .show()


""" REPLACE VALUES """

dados_manual.na.replace("job", Map("Dev Java" -> "Desenvolvedor"))
            .show()
