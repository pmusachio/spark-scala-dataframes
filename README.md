# Processing Data with Spark Dataframes

<br>

## Creating DEV environment

install packages
```zsh
brew install openjdk@11
```
```zsh
brew install scala
```
```zsh
brew install apache-spark
```

<br>

config vscode
  - install extensions `Scala Syntax (official)` and `Code Runner`

<br>

## Loading data into spark dataframes

```scala
// import functions
import org.apache.spark.sql.types.{StructField, StructType, StringType, DoubleType, IntegerType}
import org.apache.spark.sql.SparkSession

// spark session
val spark = SparkSession.builder()
                        .getOrCreate()

// load data
val df = spark.read
              .option("header","true")
              .option("inferSchema","true")
              .option("delimiter",";")
              .format("csv")
              .load("data/bank-additional-full.csv")


""" DATA EXPLORATION """"

df.printSchema()                    // return data structure
df.show()                           // shows dataframe records
df.head(10)                         // return first 10 dataframe records
df.select($"age").describe().show() // shows column statistics ("age" in example)
df.columns                          // return dataframe columns


""" WRITE DATAFRAME IN A FILE """

df.write
  .format("csv")
  .mode("overwrite")
  .option("sep", "\t")
  .save("/tmp/my-tsv-file.csv")
```

<br>

## Referencing columns


<br>

## Filtering, sorting data and applying functions


<br>

## Expressions and sampling


<br>

## Missing values ​​and replace


<br>

## Expressions and UDF


<br>

## MAP and REDUCE functions


<br>

## Joining data with JOIN


<br>

## Applying grouping functions


<br>

## ??? Introduction to spark SQL


<br>

## Data partitioning


<br>

## ??? Introduction to datasets and spark SQL


<br>

## Processing data directly in RDDs


<br>

## Shared variables


<br>

## ??? Introduction to spark SQL
