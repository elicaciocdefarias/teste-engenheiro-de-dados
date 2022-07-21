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
    .option("encoding", "ISO-8859-1")
    .csv('./work-dir/MICRODADOS_ENEM_2020.csv')
)

# colunas uteis
colunas_uteis = [
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
    "CO_MUNICIPIO_ESC",
    "NO_MUNICIPIO_ESC",
    "TP_DEPENDENCIA_ADM_ESC",
    "TP_LOCALIZACAO_ESC",
    "TP_SIT_FUNC_ESC",
    "NU_ANO",
    "TP_SEXO",
    "TP_COR_RACA",
]
df1 = df.select(colunas_uteis)
