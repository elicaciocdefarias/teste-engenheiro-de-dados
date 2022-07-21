FROM mysql:latest

COPY ./work-dir/sql/cria_tabelas.sql /docker-entrypoint-initdb.d/