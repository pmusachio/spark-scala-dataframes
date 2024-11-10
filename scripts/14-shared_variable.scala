""" Importa funções """

import org.apache.spark.sql.types.{StructField, StructType, StringType, LongType, DoubleType, IntegerType}
import org.apache.spark.sql.functions.{col, column, expr, upper, lower, desc, asc, split, udf}
import org.apache.spark.sql.Row
import org.apache.spark.util.LongAccumulator


""" Criando um Dataframe na mão """

val schema = new StructType(Array(
new StructField("job_id", IntegerType, true),
new StructField("desc", StringType, true)))

val newRows = Seq(
Row(30, "Cientista de dados"),
Row(20, "Dev Java")
)

val parallelizedRows = spark.sparkContext.parallelize(newRows)
val dados_manual = spark.createDataFrame(parallelizedRows, schema)
dados_manual.show()


""" Criando segundo Dataframe """

val schema = new StructType(Array(
new StructField("job_id", IntegerType, true),
new StructField("salary", DoubleType, true)))

val newRows = Seq(
Row(30, 10000.0),
Row(20, 9000.0),
Row(1, 15000.0),
Row(2, 2000.0),
Row(3, 3000.0)
)

val parallelizedRows = spark.sparkContext.parallelize(newRows)
val salarios = spark.createDataFrame(parallelizedRows, schema)
salarios.show()


""" Broacast dataframe """

val dados_join = dados_manual.join(salarios, dados_manual.col("job_id") === salarios.col("job_id"), "inner")
dados_join.explain

salarios.persist()
broadcast(salarios)
salarios.count()

val dados_join = dados_manual.join(salarios, dados_manual.col("job_id") === salarios.col("job_id"), "inner")
dados_join.explain


""" Broacast de outros objetos """

val filiais = Array("SP", "BH", "RJ")
val filiais_bc = sc.broadcast(filiais)
filiais_bc.value


""" Accumulators """

val contador = new LongAccumulator
spark.sparkContext.register(contador, "contador")

def contadorFunc(salario: Double) = {
    if (salario >= 10000) {
        contador.add(1)
    }
}

contador.value
contadorFunc(10000)
contador.value

salarios.foreach(row => contadorFunc(row.getAs[Double]("salary")))
contador.value
