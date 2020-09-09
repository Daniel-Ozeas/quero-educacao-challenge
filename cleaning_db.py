import psycopg2
from etl import create_db
from sql_queries import *


def droping_table(conn, cur, query):
    """
    Used to drop a table in postges database.

    Parameters:
    conn (class: psycopg2.extensions.connection): connector to postgresdb
    cur (class: psycopg2.extensions.cursor): cursor to execute SQL command
    query (str): the sql query 

    Returns:
    Nothing. Only execute the command.
    """
    try:
        cur.execute(query)
        conn.commit()
        print("Table dropped successfully in PostgreSQL")

    except (Exception, psycopg2.DatabaseError) as error:
        print('Error while droping PostgreSQL table', error) 

def main():
    conn, cur = create_db()
    droping_table(conn,cur, public_dataset_table_drop)

    if(conn):
        cur.close()
        conn.close()
        print('PostgreSQL connection is closed')

if __name__ == "__main__":
    main()