""" IMPORT FUNCTIONS """"

import org.apache.spark.sql.types.{StructField, StructType, StringType, DoubleType, IntegerType}
import org.apache.spark.sql.SparkSession


""" SPARK SESSION """

val spark = SparkSession.builder()
                        .appName("BankDataAnalysis")
                        .getOrCreate()


""" LOAD DATA """"

val dados = spark.read
                 .option("header","true")
                 .option("inferSchema","true")
                 .option("delimiter",";")
                 .format("csv")
                 .load("data/bank-additional-full.csv")


""" DATA EXPLORATION """"

dados.printSchema()                    // return data structure
dados.show(2)                          // shows dataframe records
dados.head(2)                          // return first dataframe records
dados.select($"age").describe().show() // shows column statistics ("age" in example)
dados.columns                          // return dataframe columns


""" LOAD DATA USING SCHEMA """"

// get schema
val myManualSchema = new StructType(Array(
new StructField("age", IntegerType, true),
new StructField("job", StringType, true),
new StructField("marital", StringType, true),
new StructField("education", StringType, true),
new StructField("default", StringType, true),
new StructField("housing", StringType, true),
new StructField("loan", StringType, true),
new StructField("contact", StringType, true),
new StructField("month", StringType, true),
new StructField("day_of_week", StringType, true),
new StructField("duration", StringType, true),
new StructField("campaign", IntegerType, true),
new StructField("pdays", IntegerType, true),
new StructField("previous", IntegerType, true),
new StructField("poutcome", StringType, true),
new StructField("emp.var.rate", DoubleType, true),
new StructField("cons.price.idx", DoubleType, true),
new StructField("cons.conf.idx", DoubleType, true),
new StructField("euribor3m", DoubleType, true),
new StructField("nr.employe", DoubleType, true),
new StructField("y", StringType, true)
))

// load data w/ schema
val dados_schema = spark.read
                        .option("header","true")
                        .option("delimiter",";")
                        .option("mode", "FAILFAST")
                        .schema(myManualSchema) // variable
                        .format("csv")
                        .load("data/bank-additional-full.csv")

// exploration from "dados_schema"
dados_schema.printSchema(5)
dados_schema.show(5)


""" WRITE DATAFRAME IN A FILE """

dados.write
     .format("csv")
     .mode("overwrite")
     .option("sep", "\t")
     .save("/tmp/my-tsv-file.csv")
