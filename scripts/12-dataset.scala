case class Bank (
    AGE: Int,
    JOB: String,
    MARITAL: String,
    EDUCATION: String,
    DEFAULT: String,
    HOUSING: String,
    LOAN: String,
    CONTACT: String,
    MONTH: String,
    DAY_OF_WEEK: String,
    DURATION: String,
    CAMPAIGN: Int,
    PDAYS: Int,
    PREVIOUS: Int,
    POUTCOME: String,
    `EMP.VAR.RATE`: Double,
    `CONS.PRICE.IDX`: Double,
    `CONS.CONF.IDX`: Double,
    EURIBOR3M: Double,
    `NR.EMPLOYED`: Double,
    Y: String
)


val dados = spark.read.option("header","true").option("inferSchema","true").option("delimiter",";").format("csv").load("data/bank-additional-full.csv")

val dataset = dados.as[Bank]

dados
dataset
dataset.show(5)

def ageGreaterThan40 (row: Bank): Boolean = {
    return row.AGE > 40
}

val dataset2 = dataset.filter(row => ageGreaterThan40(row))

dataset.filter(row => row.AGE < 40).show(5)

dataset.map(row => row.MARITAL).show(5)
