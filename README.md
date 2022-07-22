# Teste de Eng. de Dados
## Objetivo 
### Desenvolver um pipeline de ETL para responder às seguintes perguntas:
1. Qual a escola com a maior média de notas?
2. Qual o aluno com a maior média de notas e o valor dessa média?
3. Qual a média geral?
4. Qual o % de Ausentes?
5. Qual o número total de Inscritos?
6. Qual a média por disciplina?
7. Qual a média por Sexo?
8. Qual a média por Etnia?

### Critérios avaliadas:
- Docker;
- SQL;
- Python;
- Organização do Código
- Documentação
- ETL
- Modelagem dos dados

### Competencias Desejáveis
- PySpark
- Esquema Estrela

### Tecnologias utilizadas
- Docker;
- docker-compose;
- MySql;
- SQL;
- Python;
- PySpark

### Bibliotecas utilizadas
- pyspark
- mysql-connector-python

> Caso seja necessario instalar alguma biblioteca, use os camandos abaixo.
Lembrando que estes comandos devem ser executados no terminal do linux, caso use outro sistema operacional sinta-se a vontade para adapta-los.

```bash
pip install pyspark
```

```bash
pip install mysql-connector-python
```

### Banco de dados
- Modelagem: dimensional estrela
- Fatos: avaliacao
- Dimensoes: aluno, escola e tempo
- Tabelas: fato_avaliacao, dimensional_escola, dimensional_aluno, dimensional_tempo

#### Atributos por tabela:
- fato_avaliacao
    - ID_AVALIACAO
    - TIPO
    - NOTA
    - PRESENCA
    - ID_ESCOLA
    - ID_ALUNO
    - ID_TEMPO

- dimensional_escola
    - ID_ESCOLA
    - CODIGO_MUNICIPIO
    - NOME_MUNICIPIO
    - DEPENDENCIA
    - LOCALIZACAO
    - SITUACAO

- dimensional_aluno
    - ID_ALUNO
    - NU_INSCRICAO
    - ETNIA
    - SEXO

- dimensional_tempo
    - ID
    - ANO

## Modo de uso

1. Clone o projeto

```bash
git clone https://github.com/elicaciocdefarias/teste-engenheiro-de-dados.git
```

2. Baixe o dataset [clicando aqui](https://download.inep.gov.br/microdados/microdados_enem_2020.zip).

3. extraia o conteudo do arquivo zip e mova o dataset `MICRODADOS_ENEM_2020.csv` para a pasta `work-dir/datasets`

4. No terminal do linux, navegue ate a pasta do projeto e execute o comando abaixo para inicar o cluster do spark e o banco de dados MySql

```bash
docker-compose up --detach
```

> Atencao! O mysql costuma demorar um pouco para liberar a conexao. Por favor! Consulte os logs antes de seguir para o proximo passo.

5. Agora execute comando abaixo para iniciar o pipeline de ETL.
> Atencao! O tempo de execucao dessa tarefa depende da quantidade de recursos disponiveis do seu computador, `entao aproveita e vai tomar um cafe.`

```bash
docker-compose exec -it spark-master \
       bin/spark-submit \
       --master spark://spark-master:7077 \
       --driver-memory 1G \
       --executor-memory 1G \
       work-dir/src/ETL.py

```

6. Depois que todos os dados estiverem carregados, para conferir as resposta, abra o `Visual Studio Code` no diretorio do projeto, acesse a pasta `work-dir/notebooks` e abra o notebook `notebook_respostas`

7. Agora click pra rodar todas as colunas.

