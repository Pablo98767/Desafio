import ftplib
import os
import psycopg2
from prefect import task, Flow
#falta instalar a biblioteca abaixo
from dbt.cli import main as dbt_main

# Conectando ao servidor FTP ANS
ftp = ftplib.FTP('ftp.ans.gov.br')
ftp.login()

# Task to create table
@task
def create_materialize_table():
    connection = psycopg2.connect(
        #insira os dados do seu banco
        host='your_host',
        port='your_port',
        dbname='your_dbname',
        user='your_user',
        password='your_password'
    )
    cursor = connection.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS raw_data (
            id INTEGER,
            name VARCHAR(255),
            category VARCHAR(255),
            created_at TIMESTAMP,
            updated_at TIMESTAMP
        )
    ''')
    connection.commit()
    connection.close()

# Task para extração dos dados
@task
def extract_data():
    ftp.cwd('/Base_de_dados/Microdados/dados_dbc/beneficiarios/operadoras')
    ftp.retrbinary("RETR data.csv", open("data.csv", 'wb').write)
    ftp.quit()
    return "data.csv"

# Task para carregar os dados
@task
def load_data(data):
    connection = psycopg2.connect(
        host='localhost',
        port='5432',
        dbname='ans',
        user='postgres',
        password='123'
    )
    cursor = connection.cursor()
    cursor.execute(f"COPY raw_data FROM '{data}' WITH (FORMAT CSV, HEADER true);")
    os.remove(data)
    connection.commit()
    connection.close()

# Task para criar a tabela derivada
@task
def create_derived_table():
    dbt_main(["run", "--models", "total_plans_by_category", "--target", "postgres"])

# Criando o flow
with Flow("Data Pipeline") as flow:
    create_materialize_table()
    data = extract_data()
    load_data(data)
    create_derived_table()

# Run the flow
flow.run()
