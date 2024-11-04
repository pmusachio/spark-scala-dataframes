""" IMPORT FUNCTIONS """"

import org.apache.spark.sql.types.{StructField, StructType, StringType, LongType, DoubleType, IntegerType}
import org.apache.spark.sql.functions.{col, column, expr, upper, lower, desc, asc, split, udf}
import org.apache.spark.sql.Row


""" LOAD DATA """"

val dados = spark.read
                 .option("header","true")
                 .option("inferSchema","true")
                 .option("delimiter",";")
                 .format("csv")
                 .load("data/bank-additional-full.csv")


""" EXPRESSIONS """

// calculation expression example
dados.select($"age" + 10)
     .show()

dados.select(expr("age + 10"))
     .show(5)

// SQL expression example
dados.selectExpr(
        "*", 
        "(age > 40) as idade_maior_40")
     .show(5)

// aggregate expression example
dados.selectExpr("max(age)")
     .show()

// using reserved words
dados.selectExpr("age as `idade com espaço`")
     .show(2)


""" SPLIT DATA IN SAMPLES """

// get data sample
val seed = 2019
val withReplacement = false
val fraction = 0.1
dados.sample(withReplacement, fraction, seed)
     .count()

// split data in different samples
val seed = 2019
val dataFrames = dados.randomSplit(Array(0.25, 0.75), seed)
dataFrames(0).count()
dataFrames(1).count()


""" UNION """

val dados1 = dataFrames(0).union(dataFrames(1)).where($"marital" === "married")
dados1.show(5)


""" LIMIT """

dados1.limit(5)
