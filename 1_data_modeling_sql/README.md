# Data Modeling with Postgres

The project is about read data from files, process data to model and insert to a relational database (using Postgresql and Python)

## How to use
- Make use you have Postgres in your machine
- Change connection string with your database (in line 12 - create_tables.py and line 87 - etl.py)
- Run ```python create_tables.py``` to create database 
- Run ```python etl.py``` to transform data from files and load to database
- To check the database is exact, you can use `test.ipynb` to test.

## Project structure
```bash
workplace
|   create_tables.py
|   etl.ipynb
|   etl.py
|   README.md
|   sql_queries.py
|   test.ipynb
|   
\---data
    +---log_data
    |   \---2018
    |       \---11
    |               2018-11-01-events.json
    |               ...
    |               2018-11-30-events.json
    |               
    \---song_data
        \---A
            +---A
            |   +---A
            |   |       TRAAAAW128F429D538.json
            |   |       ...
            |   |       TRAAAVO128F93133D4.json
            |   |       
        ...

```

### Detail: 
- `create_tables.py`: script to create database, tables and relationship between tables
- `etl.ipynb`: notebook to get data, transform data and load to database 
- `etl.py`: script to get data, transform data and load to database
- `README.md`
- `sql_queries.py`: contains queries
- `test.ipynb`: check database is exact
- `Data` folder: contains song data and log data (see structure above)
