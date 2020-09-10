## Quero Educação Challenge

The goal of the challenge is to create an ETL pipeline collecting data from an API,  cleaning, and loading to a Postgres database. Furthermore, is necessary to create indexes in the Postgres table.

## API

URL: http://dataeng.quero.com:5000/caged-data

## About Files

* sql_queries.py - Contain SQL queries about the drop, creation, and data insertion used in etl.py and cleaning_db.py

* cleaning_db.py - It includes executing the query to drop the table, only use it if you wish

* etl.py - All data from API is imported, cleaned, and inserted into the Postgres table. A connection to the database is made and is created a table to the data be inserted. Furthermore, indexes are created to optimize queries.

* Quero_Educação_Interview.ipynb - The notebook contain some analysis in the dataset to verify duplicate, missing values, outliers and find the rows with more diversity of values to choose the right column to be indexed. 