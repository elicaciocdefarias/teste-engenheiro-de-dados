"""
Atenção!

Deixei todo o código aqui por se tratar de um teste e para facilitar a revisão.

"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, monotonically_increasing_id, lit


###############################################################################
###############################################################################
# ### Inicia uma sessao do spark
###############################################################################
###############################################################################
spark = (
    SparkSession.builder.appName("my spark cluster")
    .master("spark://spark-master:7077")
    .config(
        "spark.driver.extraClassPath",
        "./work-dir/jars/mysql-connector-java-8.0.29.jar",
    )
    .getOrCreate()
)

###############################################################################
###############################################################################
# ### Carrega o dataset
###############################################################################
###############################################################################
df = (
    spark.read.option("Header", True)
    .option("inferSchema", True)
    .option("delimiter", ";")
    .option("encoding", "ISO-8859-1")
    .csv("./work-dir/datasets/MICRODADOS_ENEM_2020.csv")
)

###############################################################################
###############################################################################
# ### Extrai as colunas que serão usadas
###############################################################################
###############################################################################
colunas_uteis = [
    "NU_INSCRICAO",
    "TP_PRESENCA_CN",
    "TP_PRESENCA_CH",
    "TP_PRESENCA_LC",
    "TP_PRESENCA_MT",
    "TP_STATUS_REDACAO",
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

###############################################################################
###############################################################################
# ### Extrai as colunas com as informações da escola mais o numero de inscricao
# do aluno
###############################################################################
###############################################################################
colunas = [
    "CO_MUNICIPIO_ESC",
    "NO_MUNICIPIO_ESC",
    "TP_DEPENDENCIA_ADM_ESC",
    "TP_LOCALIZACAO_ESC",
    "TP_SIT_FUNC_ESC",
    "NU_INSCRICAO",
]

df2 = df1.select(*colunas)

###############################################################################
###############################################################################
# ### Extrai as colunas com as informações da escola
###############################################################################
###############################################################################
selected_columns = [
    "CO_MUNICIPIO_ESC",
    "NO_MUNICIPIO_ESC",
    "TP_DEPENDENCIA_ADM_ESC",
    "TP_LOCALIZACAO_ESC",
    "TP_SIT_FUNC_ESC",
]

df3 = df2.select(*selected_columns).distinct().orderBy("CO_MUNICIPIO_ESC")

###############################################################################
###############################################################################
# ### Indexa o dataframe
###############################################################################
###############################################################################
df4 = df3.withColumn("ID_ESCOLA", monotonically_increasing_id())

df5 = df4.withColumn("ID_ESCOLA", df4.ID_ESCOLA + 1)

###############################################################################
###############################################################################
# ### Junta o dataframe indexado com o dataframe que contem o numero de inscricao
# do aluno
###############################################################################
###############################################################################
df6 = df2.join(
    df5,
    [
        "CO_MUNICIPIO_ESC",
        "NO_MUNICIPIO_ESC",
        "TP_DEPENDENCIA_ADM_ESC",
        "TP_LOCALIZACAO_ESC",
        "TP_SIT_FUNC_ESC",
    ],
    "left",
)

###############################################################################
###############################################################################
# ###
###############################################################################
###############################################################################
df7 = df6.na.fill(1, subset=["ID_ESCOLA"])

###############################################################################
###############################################################################
# ### Substitui os valores numericos pela descricao do items
###############################################################################
###############################################################################
def replace_tp_dependencia_adm_esc(value):
    inner_dict = {
        1: "Federal",
        2: "Estadual",
        3: "Municipal",
        4: "Privada",
    }
    return inner_dict.get(value)


def replace_tp_localizacao_esc(value):
    inner_dict = {
        1: "Urbana",
        2: "Rural",
    }
    return inner_dict.get(value)


def replace_tp_sit_func_esc(value):
    inner_dict = {
        1: "Em atividade",
        2: "Paralisada",
        3: "Extinta",
        4: "Escola extinta em anos anteriores",
    }
    return inner_dict.get(value)


udf_replace_tp_dependencia_adm_esc = udf(
    lambda x: replace_tp_dependencia_adm_esc(x)
)
udf_tp_localizacao_esc = udf(lambda x: replace_tp_localizacao_esc(x))
udf_tp_sit_func_esc = udf(lambda x: replace_tp_sit_func_esc(x))

df8 = (
    df7.withColumn(
        "TP_DEPENDENCIA_ADM_ESC",
        udf_replace_tp_dependencia_adm_esc(col("TP_DEPENDENCIA_ADM_ESC")),
    )
    .withColumn(
        "TP_LOCALIZACAO_ESC", udf_tp_localizacao_esc(col("TP_LOCALIZACAO_ESC"))
    )
    .withColumn("TP_SIT_FUNC_ESC", udf_tp_sit_func_esc(col("TP_SIT_FUNC_ESC")))
)

###############################################################################
###############################################################################
# ### Renomea as colunas do dataframe
###############################################################################
###############################################################################
df_escola = (
    df8.withColumnRenamed("CO_MUNICIPIO_ESC", "CODIGO_MUNICIPIO")
    .withColumnRenamed("NO_MUNICIPIO_ESC", "NOME_MUNICIPIO")
    .withColumnRenamed("TP_DEPENDENCIA_ADM_ESC", "DEPENDENCIA")
    .withColumnRenamed("TP_LOCALIZACAO_ESC", "LOCALIZACAO")
    .withColumnRenamed("TP_SIT_FUNC_ESC", "SITUACAO")
)

###############################################################################
###############################################################################
# ###  extrai as colunas con as informacoes do aluno
###############################################################################
###############################################################################
colunas = [
    "NU_INSCRICAO",
    "TP_SEXO",
    "TP_COR_RACA",
]

df2 = df1.select(*colunas)

###############################################################################
###############################################################################
# ###  indexa o dataframe
###############################################################################
###############################################################################
df4 = (
    df2.distinct()
    .orderBy("NU_INSCRICAO")
    .withColumn("ID_ALUNO", monotonically_increasing_id())
)

df5 = df4.withColumn("ID_ALUNO", df4.ID_ALUNO + 1)

###############################################################################
###############################################################################
# ### Substitui os valores numericos pela descricao do items
###############################################################################
###############################################################################
def replace_tp_sexo(value):
    inner_dict = {
        "M": "Masculino",
        "F": "Feminino",
    }
    return inner_dict.get(value)


def replace_tp_cor_raca(value):
    inner_dict = {
        0: "Não declarado",
        1: "Branca",
        2: "Preta",
        3: "Parda",
        4: "Amarela",
        5: "Indígena ",
    }
    return inner_dict.get(value)


udf_replace_tp_sexo = udf(lambda x: replace_tp_sexo(x))
udf_replace_tp_cor_raca = udf(lambda x: replace_tp_cor_raca(x))

df6 = df5.withColumn(
    "TP_SEXO", udf_replace_tp_sexo(col("TP_SEXO"))
).withColumn("TP_COR_RACA", udf_replace_tp_cor_raca(col("TP_COR_RACA")))

###############################################################################
###############################################################################
# ### Renomea as colunas do dataframe
###############################################################################
###############################################################################
df_aluno = df6.withColumnRenamed("TP_SEXO", "SEXO").withColumnRenamed(
    "TP_COR_RACA", "ETNIA"
)

###############################################################################
###############################################################################
# ###  extrai as colunas com as informalçoes da avaliacao
###############################################################################
###############################################################################
colunas = [
    "NU_INSCRICAO",
    "TP_PRESENCA_CN",
    "TP_PRESENCA_CH",
    "TP_PRESENCA_LC",
    "TP_PRESENCA_MT",
    "TP_STATUS_REDACAO",
    "NU_NOTA_CN",
    "NU_NOTA_CH",
    "NU_NOTA_LC",
    "NU_NOTA_MT",
    "NU_NOTA_REDACAO",
]

df2 = df1.select(*colunas)


###############################################################################
###############################################################################
# ### Substitui os valores numericos pela descricao do items
###############################################################################
###############################################################################
def replace_presenca_avaliacao(value):
    inner_dict = {
        0: "Faltou à prova",
        1: "Presente na prova",
        2: "Eliminado na prova",
    }
    return inner_dict.get(value)


def replace_presenca_redacao(value):
    inner_dict = {
        1: "Sem problemas",
        2: "Anulada",
        3: "Cópia Texto Motivador",
        4: "Em Branco",
        6: "Fuga ao tema",
        7: "Não atendimento ao tipo textual",
        8: "Texto insuficiente",
        9: "Parte desconectada",
    }
    return inner_dict.get(value)


udf_replace_presenca_avaliacao = udf(lambda x: replace_presenca_avaliacao(x))
udf_replace_presenca_redacao = udf(lambda x: replace_presenca_redacao(x))

df3 = (
    df2.withColumn(
        "TP_PRESENCA_CN", udf_replace_presenca_avaliacao(col("TP_PRESENCA_CN"))
    )
    .withColumn(
        "TP_PRESENCA_CH", udf_replace_presenca_avaliacao(col("TP_PRESENCA_CH"))
    )
    .withColumn(
        "TP_PRESENCA_LC", udf_replace_presenca_avaliacao(col("TP_PRESENCA_LC"))
    )
    .withColumn(
        "TP_PRESENCA_MT", udf_replace_presenca_avaliacao(col("TP_PRESENCA_MT"))
    )
    .withColumn(
        "TP_STATUS_REDACAO",
        udf_replace_presenca_redacao(col("TP_STATUS_REDACAO")),
    )
)

###############################################################################
###############################################################################
# ### Cria novos dataframes com as informacoes de nota, presenca e a inscricao
# do aluno
###############################################################################
###############################################################################
df_cn = (
    df3.select(
        "NU_INSCRICAO",
        "TP_PRESENCA_CN",
        "NU_NOTA_CN",
    )
    .withColumn("TIPO", lit("CN"))
    .withColumnRenamed("TP_PRESENCA_CN", "PRESENCA")
    .withColumnRenamed("NU_NOTA_CN", "NOTA")
)

df_ch = (
    df3.select(
        "NU_INSCRICAO",
        "TP_PRESENCA_CH",
        "NU_NOTA_CH",
    )
    .withColumn("TIPO", lit("CH"))
    .withColumnRenamed("TP_PRESENCA_CH", "PRESENCA")
    .withColumnRenamed("NU_NOTA_CH", "NOTA")
)

df_lc = (
    df3.select(
        "NU_INSCRICAO",
        "TP_PRESENCA_LC",
        "NU_NOTA_LC",
    )
    .withColumn("TIPO", lit("LC"))
    .withColumnRenamed("TP_PRESENCA_LC", "PRESENCA")
    .withColumnRenamed("NU_NOTA_LC", "NOTA")
)

df_mt = (
    df3.select(
        "NU_INSCRICAO",
        "TP_PRESENCA_MT",
        "NU_NOTA_MT",
    )
    .withColumn("TIPO", lit("MT"))
    .withColumnRenamed("TP_PRESENCA_MT", "PRESENCA")
    .withColumnRenamed("NU_NOTA_MT", "NOTA")
)

df_rd = (
    df3.select(
        "NU_INSCRICAO",
        "TP_STATUS_REDACAO",
        "NU_NOTA_REDACAO",
    )
    .withColumn("TIPO", lit("RD"))
    .withColumnRenamed("TP_STATUS_REDACAO", "PRESENCA")
    .withColumnRenamed("NU_NOTA_REDACAO", "NOTA")
)

###############################################################################
###############################################################################
# ### Impilha todos os dataframes
###############################################################################
###############################################################################
df_cn_ch = df_cn.union(df_ch)
df_cn_ch_lc = df_cn_ch.union(df_lc)
df_cn_ch_lc_mt = df_cn_ch_lc.union(df_mt)
df4 = df_cn_ch_lc_mt.union(df_rd)
df4 = df4.orderBy("NU_INSCRICAO")

###############################################################################
###############################################################################
# ###  indexa o dataframe
###############################################################################
###############################################################################
df5 = df4.withColumn("ID_AVALIACAO", monotonically_increasing_id())

df_avaliacao = df5.withColumn("ID_AVALIACAO", df5.ID_AVALIACAO + 1)

###############################################################################
###############################################################################
# ###  Junta os dataframes de escola, aluno e avaliacao usando o numero de
# inscricao do aluno como chave natural
###############################################################################
###############################################################################
join_1 = df_escola.join(
    df_aluno,
    ["NU_INSCRICAO"],
)

join_2 = join_1.join(
    df_avaliacao,
    ["NU_INSCRICAO"],
)

###############################################################################
###############################################################################
# ###  separa os datasets para alimentar as tabelas
###############################################################################
###############################################################################
colunas_escola = [
    "ID_ESCOLA",
    "CODIGO_MUNICIPIO",
    "NOME_MUNICIPIO",
    "DEPENDENCIA",
    "LOCALIZACAO",
    "SITUACAO",
]

colunas_aluno = [
    "ID_ALUNO",
    "NU_INSCRICAO",
    "ETNIA",
    "SEXO",
]

colunas_avaliacao = [
    "ID_AVALIACAO",
    "TIPO",
    "NOTA",
    "PRESENCA",
    "ID_ALUNO",
    "ID_ESCOLA",
]

df_dimensao_escola = join_2.select(*colunas_escola).distinct()

df_dimensao_aluno = join_2.select(*colunas_aluno).distinct()

df_fato_avaliacao = join_2.select(*colunas_avaliacao).withColumn(
    "ID_TEMPO", lit(1)
)

###############################################################################
###############################################################################
# ###  carrega os dados nas tabelas
###############################################################################
###############################################################################
####
(
    df_dimensao_escola.orderBy("ID_ESCOLA")
    .write.format("jdbc")
    .option("url", "jdbc:mysql://db/microdados")
    .option("dbtable", "dimensao_escola")
    .option("user", "microdados")
    .option("password", "microdados")
    .mode("Append")
    .save()
)

(
    df_dimensao_aluno.orderBy("ID_ALUNO")
    .write.format("jdbc")
    .option("url", "jdbc:mysql://db/microdados")
    .option("dbtable", "dimensao_aluno")
    .option("user", "microdados")
    .option("password", "microdados")
    .mode("Append")
    .save()
)

(
    spark.createDataFrame([(1, 2020)], schema="ID int, ANO int")
    .write.format("jdbc")
    .option("url", "jdbc:mysql://db/microdados")
    .option("dbtable", "dimensao_tempo")
    .option("user", "microdados")
    .option("password", "microdados")
    .mode("Append")
    .save()
)

(
    df_fato_avaliacao.orderBy("ID_AVALIACAO")
    .write.format("jdbc")
    .option("url", "jdbc:mysql://db/microdados")
    .option("dbtable", "fato_avaliacao")
    .option("user", "microdados")
    .option("password", "microdados")
    .mode("Append")
    .save()
)
