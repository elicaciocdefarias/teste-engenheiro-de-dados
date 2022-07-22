WITH base AS (
    SELECT
        de.ID_ESCOLA AS ID_ESCOLA,
        de.CODIGO_MUNICIPIO AS CODIGO_MUNICIPIO,
        de.NOME_MUNICIPIO AS NOME_MUNICIPIO,
        de.DEPENDENCIA AS DEPENDENCIA,
        de.LOCALIZACAO AS LOCALIZACAO,
        de.SITUACAO AS SITUACAO,
        AVG(fa.NOTA) AS MEDIA
    FROM                                                                                                                                                                                                                                                                                                            
        fato_avaliacao AS fa
    JOIN 
        dimensao_escola AS de
    ON
        fa.ID_ESCOLA = de.ID_ESCOLA
    WHERE fa.NOTA IS NOT NULL
    GROUP BY
        de.ID_ESCOLA ,
        de.CODIGO_MUNICIPIO ,
        de.NOME_MUNICIPIO ,
        de.DEPENDENCIA ,
        de.LOCALIZACAO ,
        de.SITUACAO
    ORDER BY AVG(fa.NOTA) DESC
)
SELECT
    ID_ESCOLA,
    CODIGO_MUNICIPIO,
    NOME_MUNICIPIO,
    DEPENDENCIA,
    LOCALIZACAO,
    SITUACAO,
    MEDIA
FROM
    base
LIMIT 1