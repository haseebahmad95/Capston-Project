
# Introduction 
This Udacity Capstone Project is for the comprehensive hands on experience of the concepts which was learnt in this Nano degree program which includes data modeling, creating a dataware house etc. 
In this project two data sources are used (netflix movies data, imdb movies data). The data is transformed to star schema for data modeling to efficiently perform the analysis on netflix movies data.
 A dataware house is created in Amazon redshift using python. Netflix movies data has been downloaded from [kaggle](https://www.kaggle.com/shivamb/netflix-shows) and imdb datasets are taken from [imdb official datasets](https://datasets.imdbws.com/). The data is kept on s3 to copy into redshift. This database includes 7 dimension tables, 5 fact tables and 3 stagging tables.
 
This project has the following files and folder
| File	           |  Description  |  
|------------------|---------------|
| create_tables.py |  Drop and create tables which are defined in the sql_queries.py | 
| sql_queries.py         |DDL scripts of all the tables and insertion and selection scripts   | 
| etl.py |  Contains all the etl operations to fill the dimension and facts table
| Analytical_Queries.ipynb |  Contains all the sample analytical queries with graph plots
| create_aws_resources.py |  To create resources on AWS (e.g IAM role, redshift clustor, security group)
| delete_aws_resources.py |  Delete resources from AWS to avoid over billing
| dwh.cfg |  Contains all the configurations like (AWS key/Secret, Redshift related configurations, s3 bucket paths etc.) 

# Purpose of Netflix movies database
The purpose of this database is to design a schema in such a way that the analytical queries can be efficiently performed and we can analysis the netflix movies better. 

# Datasources

- [Netflix movies data from kaggle](https://www.kaggle.com/shivamb/netflix-shows)
- [IMDB movies data](https://datasets.imdbws.com/)

# Scope of the project:

In this project data is gathered from two sources netflix data and imdb datato better analyse the movies on netflix the purpose of the incorporatingimdb data is to analyze the movies with respect to their ratings. 

# Addressing Other Scenarios:
Raw data will be maintained on s3 bucket to manage the 100x speed. And dataware is maintained in AWS Redshift which is optimized for fast read operations and could be scaled up multiple nodes if the 100+ people try toaccess it.

# Data cleansing considerations:

As the imdb is very bulk as compare to the netlix data and there are multiple duplications in the imdb dataset to match the data in bases of movies title. so to narrow down the scope of the project only those movies kept in the stagging table whose release year is greater then 2000 and only movies and tvSeries data is considered for anlysis.

# Schema
This database has the star schema by design which includes

7 dimension tables: 
    
    - dim_type
    - dim_country 
    - dim_date
    - dim_director
    - dim_actor 
    - dim_movie_title
    - dim_category

5 fact tables: 

    - fact_movies_added_on_netflix
    - fact_movies_direction_on_netflix
    - fact_movies_actor_on_netflix
    - fact_movies_country_on_netflix
    - fact_movies_category_on_netflix

3 stagging tables:

    - stagging_netflix_movies_data
    - stagging_imdb_movies_data
    - stagging_imdb_movies_rating_data
    

![DB Schema](https://github.com/haseebahmad95/Capston-Project/blob/master/Capston_Project.jpg)

# Install the dependencies
``` bash
pip install -r requirements.txt
```

# Create a Redshift Cluster and IAM role
``` bash
# -First set the aws configurations in the dwh.cfg file
python create_aws_resources.py
```
# Steps to perform ETL task.
In order to run etl pipline run the following commands
```bash
# -Set the reshift related configuration in the dwh.cfg file
python create_tables.py 
python etl.py 
```
# Deallocate the AWS resources
``` bash
# -Delete all the aws resources
python delete_aws_resources.py
```

# Example Analytical Queries

Example analytical queries has been enlisted in the Analytical_Queries.ipynb

