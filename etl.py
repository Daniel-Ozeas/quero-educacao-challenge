# Import libs
import requests
import json
import pandas as pd
import urllib3
from urllib3 import request
import certifi
import psycopg2
from psycopg2 import Error
import matplotlib.pyplot as plt
from sql_queries import *

def get_data_from_api(url):
    """
    Used to get data from a API.

    Parameters:
    url (str): API url

    Returns:
    df (pandas.core.frame.DataFrame): Data imported from API
    """

    http = urllib3.PoolManager(
            cert_reqs='CERT_REQUIRED',
            ca_certs=certifi.where()
            )

    response = http.request('GET', url)

    data = json.loads(response.data.decode('utf-8'))

    # in this dataset, the data to extract is under 'caged'
    df = pd.json_normalize(data, 'caged')

    return df


def creating_table(conn, cur, query):
    """
    Used to create a table in db

    Parameters:
    conn (class: psycopg2.extensions.connection): connector to db
    cur (class: psycopg2.extensions.cursor): cursor to execute SQL command
    query (str): the sql query 

    Returns:
    Nothing. Only execute the command.
    """
    try:
        cur.execute(query)
        conn.commit()
        print("Table created successfully in PostgreSQL")

    except (Exception, psycopg2.DatabaseError) as error:
        print('Error while creating PostgreSQL table', error) 


def inserting_data_table(conn, cur, query, df):
    """
    Used to create a table in postgres db

    Parameters:
    conn (class: psycopg2.extensions.connection): connector to db
    cur (class: psycopg2.extensions.cursor): cursor to execute SQL command
    query (str): the sql query 
    df (list): data cleaned

    Returns:
    Nothing. Only execute the command.
    """

    try:
        cur.executemany(query, df)
        conn.commit()

        print('Data was inserted sucessfully')
        
    except (Exception, psycopg2.Error) as error:
        print('Error while inserting data:', error)

def cleaning_data(df):
    """
    Used to clean the data in postgres db from API 'http://dataeng.quero.com:5000/caged-data'

    Parameters: 
    df (list): Data to be cleaned

    Returns:
    Data cleaned
    """

    # Remove commas from salario column to be changed its data type
    df['salario'] = df['salario'].str.replace(',', '')

    # Correct data type
    df['categoria'] = df['categoria'].astype('int')
    df['cbo2002_ocupacao'] = df['cbo2002_ocupacao'].astype('int')
    df['competencia'] = df['competencia'].astype('int')
    df['fonte'] = df['fonte'].astype('int')
    df['grau_de_instrucao'] = df['grau_de_instrucao'].astype('int')
    df['horas_contratuais'] = df['horas_contratuais'].astype('int')
    df['id'] = df['id'].astype('int')
    df['idade'] = df['idade'].astype('int')
    df['ind_trab_intermitente'] = df['ind_trab_intermitente'].astype('int')
    df['ind_trab_parcial'] = df['ind_trab_parcial'].astype('int')
    df['indicador_aprendiz'] = df['indicador_aprendiz'].astype('int')
    df['municipio'] = df['municipio'].astype('int')
    df['raca_cor'] = df['raca_cor'].astype('int')
    df['regiao'] = df['regiao'].astype('int')
    df['salario'] = df['salario'].astype('float')
    df['saldo_movimentacao'] = df['saldo_movimentacao'].astype('int')
    df['secao'] = df['secao'].astype('object')
    df['sexo'] = df['sexo'].astype('int')
    df['subclasse'] = df['subclasse'].astype('int')
    df['tam_estab_jan'] = df['tam_estab_jan'].astype('int')
    df['tipo_de_deficiencia'] = df['tipo_de_deficiencia'].astype('int')
    df['tipo_empregador'] = df['tipo_empregador'].astype('int')
    df['tipo_estabelecimento'] = df['tipo_estabelecimento'].astype('int')
    df['tipo_movimentacao'] = df['tipo_movimentacao'].astype('int')
    df['uf'] = df['uf'].astype('int')

    # Round salario column to 3 decimals
    df['salario'] = df['salario'].round(decimals=3)

    # Remove some outliers from uf column
    indexNames = df[df['uf'] == 99].index
    df.drop(indexNames, inplace=True)

    # Remove some outliers from salario column
    indexNames = df[df['salario'] > 1.5e+06].index
    df.drop(indexNames, inplace=True)

    # Transform data to list to be exported to db
    df = pd.DataFrame(df)
    df = df.values.tolist()
    
    return df

def create_db():
    """
    Create the connection and the cursor to the Postgres db 

    Parameters: 
    

    Returns:
    Nothing. Just execute the command.
    """
    try:
        conn = psycopg2.connect(user='postgres',
                                password='postgres',
                                host = '127.0.0.1',
                                port = '5432',
                                database = 'postgres')
        cur = conn.cursor()
        return conn, cur

    except (Exception, psycopg2.DatabaseError) as error:
        print('Error while connecting to PostgreSQL db', error)

def creating_index(conn, cur, index_columns):
    """
    Create the index in postgres table 

    Parameters: 
    conn (class: psycopg2.extensions.connection): connector to Postgres database
    cur (class: psycopg2.extensions.cursor): cursor to execute SQL command
    index_columns (list): list of desired indexe column 

    Returns:
    Nothing. Just execute the command.
    """
    try:        
        for index_column in index_columns:
            index_query = f"""
            CREATE INDEX idx_{index_column} 
            ON public_dataset({index_column});
            """
            cur.execute(index_query)
            conn.commit()
            print(f'Index idx_{index_column} created sucessfully')
        
    except (Exception, psycopg2.Error) as error:
        print('Error while creating index:', error)        

def main():
    """
    Executes function to get data, clean, connect to db, creates table in postgres db, inserts the data cleaned and creates indexes.

    Parameters: 


    Returns:
    Nothing. Just execute the command.
    """

    # Get data from API
    data = get_data_from_api('http://dataeng.quero.com:5000/caged-data')
    print('Data imported from api')
    data_cleaned = cleaning_data(data)
    print('Data cleaned')

    # Access database, create table and insert the data
    conn, cur = create_db()
    creating_table(conn, cur, public_dataset_table_create)
    inserting_data_table(conn, cur, public_dataset_table_insert, data_cleaned)
    index_columns = ['cbo2002_ocupacao', 'salario', 'municipio', 'subclasse']
    creating_index(conn, cur, index_columns)

    if(conn):
        cur.close()
        conn.close()
        print('PostgreSQL connection is closed')

if __name__ == "__main__":
    main()