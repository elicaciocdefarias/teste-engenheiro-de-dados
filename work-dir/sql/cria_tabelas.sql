use microdados;

CREATE TABLE IF NOT EXISTS dimensao_escola (
    ID_ESCOLA bigint NOT NULL AUTO_INCREMENT,
    CODIGO_MUNICIPIO int,
    NOME_MUNICIPIO varchar(255),  
    DEPENDENCIA varchar(255),
    LOCALIZACAO varchar(255),
    SITUACAO varchar(255),
    PRIMARY KEY (ID_ESCOLA)
);

CREATE TABLE IF NOT EXISTS dimensao_aluno (
    ID_ALUNO bigint NOT NULL AUTO_INCREMENT,
    NU_INSCRICAO bigint, 
    ETNIA varchar(255),
    SEXO varchar(255),
    PRIMARY KEY (ID_ALUNO)
);

CREATE TABLE IF NOT EXISTS dimensao_tempo (
    ID int NOT NULL AUTO_INCREMENT,
    ANO int NOT NULL,
    PRIMARY KEY (ID)
);

CREATE TABLE IF NOT EXISTS fato_avaliacao (
    ID_AVALIACAO bigint NOT NULL AUTO_INCREMENT,
    TIPO varchar(255),
    NOTA double(255, 1),
    PRESENCA varchar(255),
    ID_ESCOLA bigint,
    ID_ALUNO bigint,
    ID_TEMPO int,
    PRIMARY KEY (ID_AVALIACAO),
    FOREIGN KEY (ID_ESCOLA) REFERENCES dimensao_escola(ID_ESCOLA),
    FOREIGN KEY (ID_ALUNO) REFERENCES dimensao_aluno(ID_ALUNO),
    FOREIGN KEY (ID_TEMPO) REFERENCES dimensao_tempo(ID)
);