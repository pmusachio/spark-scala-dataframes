""" Importando funcoes e classes """

import org.apache.spark.sql.functions.{max, min, count, sum, avg, countDistinct, approx_count_distinct, first, last, sumDistinct}
import org.apache.spark.sql.functions.{desc, expr, col, to_date, grouping_id}
import org.apache.spark.sql.functions.{var_pop, stddev_pop, var_samp, stddev_samp, skewness, kurtosis, corr, covar_pop, covar_samp}
import org.apache.spark.sql.functions.{collect_set, collect_list}
import org.apache.spark.sql.functions.{dense_rank, rank, row_number, ntile}
import org.apache.spark.sql.expressions.{Window, MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._


""" Lendo dados """

val dataset = spark.read.option("header", "true").option("inferSchema", "true").option("delimiter", ";").csv("data/bank-additional-full.csv")
dataset.printSchema


""" Usando funcoes de agrupamento """

dataset.groupBy($"job").agg(avg("age")).show()

dataset.groupBy("job").agg(avg("age").alias("idade")).show()

dataset.groupBy("job").agg(avg("age").alias("idade")).orderBy(desc("idade")).limit(5).show()

dataset.groupBy("job").agg(min("age").alias("min_age"), max("age").alias("max_age")).show()

dataset.groupBy("job").agg("age"-> "min", "age" -> "max").show()

dataset.groupBy("job").agg(approx_count_distinct("age", 0.1)).show()

dataset.select(first("age"), last("age")).show()

dataset.select(expr("max(age)")).show()

dataset.groupBy("job").agg(collect_set("education"), collect_list("education")).show()

val dataset1 = dataset.groupBy($"y").agg(count($"y").alias("count_class"))
dataset1.select($"y", $"count_class").withColumn("percentage", col("count_class") / sum("count_class").over()).show()


""" Trabalhando com Window functions """

val df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("../guide/book/data/retail-data/all/*.csv").coalesce(5)
val dfWithDate = df.withColumn("date", to_date(col("InvoiceDate"), "MM/d/yyyy H:mm"))
val windowSpec = Window.partitionBy("CustomerId", "date").orderBy(col("Quantity").desc).rowsBetween(Window.unboundedPreceding, Window.currentRow)
val maxPurchaseQuantity = max(col("Quantity")).over(windowSpec)
val purchaseDenseRank = dense_rank().over(windowSpec)
val purchaseRank = rank().over(windowSpec)

dfWithDate.where("CustomerId IS NOT NULL").orderBy("CustomerId")
.select(
col("CustomerId"),
col("date"),
col("Quantity"),
purchaseRank.alias("quantityRank"),
purchaseDenseRank.alias("quantityDenseRank"),
maxPurchaseQuantity.alias("maxPurchaseQuantity")).show()


""" in SQL
SELECT
    CustomerId,
    date,
    Quantity,
    rank(Quantity) OVER (PARTITION BY CustomerId, date ORDER BY Quantity DESC NULLS LAST ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as rank,
    dense_rank(Quantity) OVER (PARTITION BY CustomerId, date ORDER BY Quantity DESC NULLS LAST ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as dRank,
    max(Quantity) OVER (PARTITION BY CustomerId, date ORDER BY Quantity DESC NULLS LAST ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as maxPurchase
FROM
    dfWithDate
WHERE
    CustomerId IS NOT NULL
ORDER BY
    CustomerId
"""


""" Grouping sets """

val dfNoNull = dfWithDate.drop()
dfNoNull.createOrReplaceTempView("dfNoNull")

spark.sql("SELECT CustomerId, stockCode, sum(Quantity) FROM dfNoNull
GROUP BY customerId, stockCode GROUPING SETS((customerId, stockCode))
ORDER BY CustomerId DESC, stockCode DESC")

spark.sql("SELECT CustomerId, stockCode, sum(Quantity) FROM dfNoNull
GROUP BY customerId, stockCode GROUPING SETS((customerId, stockCode),())
ORDER BY CustomerId DESC, stockCode DESC")

val rolledUpDF = dfNoNull.rollup("Date", "Country").agg(sum("Quantity")).selectExpr("Date", "Country", "`sum(Quantity)` as total_quantity").orderBy("Date")
rolledUpDF.show()

rolledUpDF.where("Country IS NULL").show()
rolledUpDF.where("Date IS NULL").show()

dfNoNull.cube("Date", "Country").agg(sum(col("Quantity"))).select("Date", "Country", "sum(Quantity)").orderBy("Date").show()

dfNoNull.cube("customerId", "stockCode").agg(grouping_id(), sum("Quantity")).orderBy(expr("grouping_id()").desc).show()

val pivoted = dfWithDate.groupBy("date").pivot("Country").sum()
pivoted.where("date > '2011-12-05'").select("date" ,"`USA_sum(Quantity)`").show()


""" User-Defined Aggregation Functions (UDAFs) """

class BoolAnd extends UserDefinedAggregateFunction {
    def inputSchema: org.apache.spark.sql.types.StructType =
        StructType(StructField("value", BooleanType) :: Nil)

    def bufferSchema: StructType = StructType(
        StructField("result", BooleanType) :: Nil
    )

    def dataType: DataType = BooleanType
    def deterministic: Boolean = true
    def initialize(buffer: MutableAggregationBuffer): Unit = {
        buffer(0) = true
    }
    def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
        buffer(0) = buffer.getAs[Boolean](0) && input.getAs[Boolean](0)
    }
    def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
        buffer1(0) = buffer1.getAs[Boolean](0) && buffer2.getAs[Boolean](0)
    }
    def evaluate(buffer: Row): Any = {
        buffer(0)
    }
}

val ba = new BoolAnd
spark.udf.register("booland", ba)

val df1 = spark.range(1).selectExpr("explode(array(TRUE, TRUE, TRUE)) as t").selectExpr("explode(array(TRUE, FALSE, TRUE)) as f", "t")
df1.show()
df1.select(ba(col("t")), expr("booland(f)")).show()
