from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, monotonically_increasing_id, lit


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

# extrai as informacoes da escola junto com o numero
# de inscricao do aluno
colunas = [
    "CO_MUNICIPIO_ESC",
    "NO_MUNICIPIO_ESC",
    "TP_DEPENDENCIA_ADM_ESC",
    "TP_LOCALIZACAO_ESC",
    "TP_SIT_FUNC_ESC",
    "NU_INSCRICAO",
]
df2 = df1.select(*colunas)

# remove as linhas onde todas as informacoes da escola estao vazias
# remove as linhas duplicadas
# ordena pelo codigo do municipio
# cria um novo dataframe
colunas_escola = [
    "CO_MUNICIPIO_ESC",
    "NO_MUNICIPIO_ESC",
    "TP_DEPENDENCIA_ADM_ESC",
    "TP_LOCALIZACAO_ESC",
    "TP_SIT_FUNC_ESC",   
]

df3 = (
    df2
    .dropna(
        how="all", 
        subset=colunas_escola
    )
    .distinct()
    .orderBy("CO_MUNICIPIO_ESC")
)

# indexa as linhas
df4 = df3.withColumn(
    "ID_ESCOLA", monotonically_increasing_id()
)

df5 = df4.withColumn(
    "ID_ESCOLA", df4.ID_ESCOLA +1
)

# substitui os valor numericos pelas descricoes
def replace_tp_dependencia_adm_esc(value):
    inner_dict = {
        1 : "Federal",
        2 : "Estadual",
        3 : "Municipal",
        4 : "Privada",
    }
    return inner_dict[value]

def replace_tp_localizacao_esc(value): 
    inner_dict = {
        1: "Urbana",
        2: "Rural",
    }
    return inner_dict[value]

def replace_tp_sit_func_esc(value):
    inner_dict ={
        1 : "Em atividade",
        2 : "Paralisada",
        3 : "Extinta",
        4 : "Escola extinta em anos anteriores",
    }
    return inner_dict[value]

udf_replace_tp_dependencia_adm_esc = udf(lambda x: replace_tp_dependencia_adm_esc(x))
udf_tp_localizacao_esc = udf(lambda x: replace_tp_localizacao_esc(x))
udf_tp_sit_func_esc = udf(lambda x: replace_tp_sit_func_esc(x))

df6 = (
    df5
    .withColumn(
        "TP_DEPENDENCIA_ADM_ESC", 
        udf_replace_tp_dependencia_adm_esc(
            col("TP_DEPENDENCIA_ADM_ESC")
        )
    )
    .withColumn(
        "TP_LOCALIZACAO_ESC", 
        udf_tp_localizacao_esc(
            col("TP_LOCALIZACAO_ESC")
        )
    )
    .withColumn(
        "TP_SIT_FUNC_ESC", 
        udf_tp_sit_func_esc(
            col("TP_SIT_FUNC_ESC")
        )
    )
)

df_escola = (
    df6
    .withColumnRenamed("CO_MUNICIPIO_ESC", "CODIGO_MUNICIPIO")
    .withColumnRenamed("NO_MUNICIPIO_ESC", "NOME_MUNICIPIO")
    .withColumnRenamed("TP_DEPENDENCIA_ADM_ESC", "DEPENDENCIA")
    .withColumnRenamed("TP_LOCALIZACAO_ESC", "LOCALIZACAO")
    .withColumnRenamed("TP_SIT_FUNC_ESC", "SITUACAO")
)

#### alunos

# extrai as informacoes do aluno
colunas = [
    "NU_INSCRICAO",
    "TP_SEXO",
    "TP_COR_RACA",
]

df2 = df1.select(*colunas)

# remove as linhas onde todas as informacoes da escola estao vazias
# remove as linhas duplicadas
# ordena pelo codigo do municipio
# cria um novo dataframe
colunas_aluno = [
    "TP_SEXO",
    "TP_COR_RACA",
]

df3 = (
    df2
    .dropna(
        how="all", 
        subset=colunas_aluno
    )
    .distinct()
    .orderBy("NU_INSCRICAO")
)

# indexa as linhas
df4 = df3.withColumn(
    "ID_ALUNO", monotonically_increasing_id()
)

df5 = df4.withColumn(
    "ID_ALUNO", df4.ID_ALUNO +1
)

# substitui os valor numericos pelas descricoes
def replace_tp_sexo(value):
    inner_dict = {
        "M" : "Masculino",
        "F" : "Feminino", 
    }
    return inner_dict[value]

def replace_tp_cor_raca(value): 
    inner_dict = {
        0 : "Não declarado",
        1 : "Branca",
        2 : "Preta",
        3 : "Parda",
        4 : "Amarela",
        5 : "Indígena ",

    }
    return inner_dict[value]

udf_replace_tp_sexo = udf(lambda x: replace_tp_sexo(x))
udf_replace_tp_cor_raca = udf(lambda x: replace_tp_cor_raca(x))

df6 = (
    df5
    .withColumn(
        "TP_SEXO", 
        udf_replace_tp_sexo(
            col("TP_SEXO")
        )
    )
    .withColumn(
        "TP_COR_RACA", 
        udf_replace_tp_cor_raca(
            col("TP_COR_RACA")
        )
    )
)

df_aluno = (
    df6
    .withColumnRenamed("TP_SEXO", "SEXO")
    .withColumnRenamed("TP_COR_RACA", "ETNIA")
)

#### avaliacao

# extrai as informacoes das avaliacao
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

# substitui os valor numericos pelas descricoes
def replace_presenca_avaliacao(value):
    inner_dict = {
        0: "Faltou à prova",
        1: "Presente na prova",
        2: "Eliminado na prova",
    }
    return inner_dict[value]

def replace_presenca_redacao(value):
    inner_dict = {
        1 : "Sem problemas",
        2 : "Anulada",
        3 : "Cópia Texto Motivador",
        4 : "Em Branco",
        6 : "Fuga ao tema",
        7 : "Não atendimento ao tipo textual",
        8 : "Texto insuficiente",
        9 : "Parte desconectada",
    }
    return inner_dict.get(value, "Não Informada")

udf_replace_presenca_avaliacao = udf(lambda x: replace_presenca_avaliacao(x))
udf_replace_presenca_redacao = udf(lambda x: replace_presenca_redacao(x))

df3 = (
    df2
    .withColumn(
        "TP_PRESENCA_CN", 
        udf_replace_presenca_avaliacao(
            col("TP_PRESENCA_CN")
        )
    )
    .withColumn(
        "TP_PRESENCA_CH", 
        udf_replace_presenca_avaliacao(
            col("TP_PRESENCA_CH")
        )
    )
    .withColumn(
        "TP_PRESENCA_LC", 
        udf_replace_presenca_avaliacao(
            col("TP_PRESENCA_LC")
        )
    )
    .withColumn(
        "TP_PRESENCA_MT", 
        udf_replace_presenca_avaliacao(
            col("TP_PRESENCA_MT")
        )
    )
    .withColumn(
        "TP_STATUS_REDACAO", 
        udf_replace_presenca_redacao(
            col("TP_STATUS_REDACAO")
        )
    )
)

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

df_cn_ch = df_cn.union(df_ch)
df_cn_ch_lc = df_cn_ch.union(df_lc)
df_cn_ch_lc_mt = df_cn_ch_lc.union(df_mt)
df4 = df_cn_ch_lc_mt.union(df_rd)
df4 = df4.orderBy("NU_INSCRICAO")

# indexa as linhas
df5 = df4.withColumn(
    "ID_AVALIACAO", monotonically_increasing_id()
)

df_avaliacao = df5.withColumn(
    "ID_AVALIACAO", df5.ID_AVALIACAO +1
)

#### join dataframes

join_1 = (
    df_escola
    .join(
        df_aluno,
        ["NU_INSCRICAO"],
        "right"
    )
)

join_2 = (
    join_1.join(
        df_avaliacao,
        ["NU_INSCRICAO"],
        "left"
    )
)

#### separa os datasets para alimentar as tabelas

colunas_escola = [
    'ID_ESCOLA', 
    'CODIGO_MUNICIPIO',
    'NOME_MUNICIPIO',
    'DEPENDENCIA',
    'LOCALIZACAO',
    'SITUACAO',
]

colunas_aluno = [
    'ID_ALUNO',
    'NU_INSCRICAO',  
    'ETNIA',
    'SEXO',
]

colunas_avaliacao = [
    'ID_AVALIACAO',
    'TIPO',
    'NOTA',
    'PRESENCA',
    'ID_ALUNO',
    'ID_ESCOLA',
 ]

df_dimensao_escola = (
    join_2
    .select(*colunas_escola)
    .distinct()
)

df_dimensao_escola.show(5)

df_dimensao_aluno = (
    join_2
    .select(*colunas_aluno)
    .distinct()
)

df_dimensao_aluno.show(5)

df_fato_avaliacao = (
    join_2
    .select(*colunas_avaliacao)
)

df_fato_avaliacao.show(5)