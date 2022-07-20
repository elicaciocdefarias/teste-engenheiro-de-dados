from pyspark.sql import SparkSession

# pega uma sessao do spark
spark = (
    SparkSession
    .builder
    .appName('my spark cluster')
    .master('spark://spark-master:7077')
    .config("spark.driver.extraClassPath", "./work-dir/mysql-connector-java-8.0.29.jar")
    .getOrCreate()
)

# carrega os dados
df = (
    spark
    .read
    .option('Header', True)
    .option('inferSchema', True)
    .option("delimiter", ";")
    .csv('./work-dir/MICRODADOS_ENEM_2020.csv')
)

# colunas uteis
columns = [
    "NU_INSCRICAO",
    "TP_PRESENCA_CN",
    "TP_PRESENCA_CH",
    "TP_PRESENCA_LC",
    "TP_PRESENCA_MT",
    "NU_NOTA_CN",
    "NU_NOTA_CH",
    "NU_NOTA_LC",
    "NU_NOTA_MT",
    "NU_NOTA_REDACAO",
    "TP_SEXO",
    "TP_COR_RACA",
    "NU_ANO"
]

df1 = df.select(columns)
