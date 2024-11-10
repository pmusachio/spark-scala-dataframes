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


""" FILTER AND ORDER BY """

// filter age > 55 and order by age desc
dados.select($"age", $"job")
     .filter($"age" > 55)
     .orderBy($"age".desc)
     .show(2)

// filter marital = married
dados.select($"age", $"marital")
     .filter($"marital" === "married")
     .show(5)

dados.select($"age", $"marital")
     .filter($"marital".equalTo("married"))
     .show(5)

// filter marital <> married
dados.select($"age", $"marital")
     .where($"marital" =!= "married")
     .show(5) //scala stype

dados.select($"age", $"marital")
     .where("marital <> 'married' ")
     .show(5) // SQL stype

// show unique values
dados.select($"marital")
     .distinct()
     .show()

// using multiple filters
val filtro_idade = col("age") > 40
val filtro_civil = col("marital").contains("married")

dados.select($"age", $"marital", $"job")
     .where(col("job")
     .isin("unemployed", "retired"))
     .where(filtro_civil.or(filtro_idade))
     .show(5)

// filters can be used in select too
dados.select($"age", $"marital", $"job")
     .where(col("job").isin("unemployed", "retired"))
     .where(filtro_civil.or(filtro_idade))
     .withColumn("filtro_civil", filtro_civil)
     .show(5)


""" DATA CONVERSIONS """

// set age as a string
val dados1 = dados.withColumn("idade_string", col("age").cast("string"))
dados1.select($"idade_string")


""" FUNCTIONS """

dados.select(upper($"poutcome")).show(1) // uppercase string
dados.select(lower($"poutcome")).show(1) // lowercase string
