from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, monotonically_increasing_id


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
    "ID", monotonically_increasing_id()
)

df5 = df4.withColumn(
    "ID", df4.ID +1
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

df_escola = (
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

print(df_escola.show(5))

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
    "ID", monotonically_increasing_id()
)

df5 = df4.withColumn(
    "ID", df4.ID +1
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

df_aluno = (
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

df_aluno.show(5)
