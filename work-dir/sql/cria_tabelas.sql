use microdados;

CREATE TABLE IF NOT EXISTS dimensao_escola (
    ID int NOT NULL AUTO_INCREMENT,
    ID_ESCOLA int,
    CODIGO_MUNICIPIO int,
    NOME_MUNICIPIO varchar(255),  
    DEPENDENCIA varchar(255),
    LOCALIZACAO varchar(255),
    SITUACAO varchar(255),
    PRIMARY KEY (ID)
);

CREATE TABLE IF NOT EXISTS dimensao_aluno (
    ID int NOT NULL AUTO_INCREMENT,
    ID_ALUNO int,
    NU_INSCRICAO int, 
    ETNIA varchar(255),
    SEXO varchar(255),
    PRIMARY KEY (ID)
);

CREATE TABLE IF NOT EXISTS dimensao_tempo (
    ID int NOT NULL AUTO_INCREMENT,
    ANO int NOT NULL,
    PRIMARY KEY (ID)
);

CREATE TABLE IF NOT EXISTS fato_avaliacao (
    ID int NOT NULL AUTO_INCREMENT,
    ID_AVALIACAO int,
    TIPO varchar(255),
    NOTA double(255, 1),
    PRESENCA varchar(255),
    ID_ESCOLA int,
    ID_ALUNO int,
    ID_TEMPO int,
    PRIMARY KEY (ID),
    FOREIGN KEY (ID_ESCOLA) REFERENCES dimensao_escola(ID),
    FOREIGN KEY (ID_ALUNO) REFERENCES dimensao_aluno(ID),
    FOREIGN KEY (ID_TEMPO) REFERENCES dimensao_tempo(ID)
);