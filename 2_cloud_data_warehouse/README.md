# Data Warehouse with AWS

The project is about create a relational database from scratch and save it to AWS 
When you save your database on AWS, you can easily access, add, modify and analysis it.

## How to use
- In 'dwh.cfg', you need to change your aws config.
- Run ```python create_tables.py``` to create database 
- Run ```python etl.py``` to transform data from files and load to database


## Data Structure
- Fact Table: 
    - songplays
        - songplay_id
        - start_time
        - user_id
        - level
        - song_id
        - artist_id
        - session_id
        - location
        - user_agent
        
- Dim Table:
    - artists
        - artist_id
        - name
        - location
        - lattitude
        - longitude
        
    - songs
        - song_id
        - title
        - artist_id
        - year
        - duration
        
    - users
        - user_id
        - first_name
        - last_name
        - gender
        - level
        
    - time
        - start_time
        - hour
        - day
        - week
        - month
        - year
        - weekday
    
## Technology
- Data Warehouse: AWS Redshift
- Data Storage Service: AWS S3 
- Python Programing Language

