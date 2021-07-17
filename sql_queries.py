import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')
KEY                     = config.get('AWS','KEY')
SECRET                  = config.get('AWS','SECRET')
ARN                     = config.get('IAM_ROLE','ARN')
NETFLIX_MOVIES_DATA     = config.get('S3','NETFLIX_MOVIES_DATA')
IMDB_MOVIES_DATA        = config.get('S3','IMDB_MOVIES_DATA')
IMDB_MOVIES_RATING_DATA = config.get('S3','IMDB_MOVIES_RATING_DATA')

# DROP TABLES

stagging_netflix_movies_data_table_drop = "DROP TABLE IF EXISTS stagging_netflix_movies_data"
stagging_imdb_movies_data_table_drop = "DROP TABLE IF EXISTS stagging_imdb_movies_data"
stagging_imdb_movies_rating_data_table_drop = "DROP TABLE IF EXISTS stagging_imdb_movies_rating_data"
dim_type_table_drop = "DROP TABLE IF EXISTS dim_type"
dim_country_table_drop = "DROP TABLE IF EXISTS dim_country"
dim_date_table_drop = "DROP TABLE IF EXISTS dim_date"
dim_director_table_drop = "DROP TABLE IF EXISTS dim_director"
dim_actor_table_drop = "DROP TABLE IF EXISTS dim_actor"
dim_category_table_drop = "DROP TABLE IF EXISTS dim_category"
dim_movie_title_table_drop = "DROP TABLE IF EXISTS dim_movie_title"
fact_movies_added_on_table_drop = "DROP TABLE IF EXISTS fact_movies_added_on_netflix"
fact_movies_direction_on_netflix_table_drop = "DROP TABLE IF EXISTS fact_movies_direction_on_netflix"
fact_movies_actor_on_netflix_table_drop = "DROP TABLE IF EXISTS fact_movies_actor_on_netflix"
fact_movies_country_on_netflix_table_drop = "DROP TABLE IF EXISTS fact_movies_country_on_netflix"
fact_movies_category_on_netflix_table_drop = "DROP TABLE IF EXISTS fact_movies_category_on_netflix"


# CREATE TABLES

stagging_netflix_movies_data_table_create = ("""
    CREATE TABLE if not exists stagging_netflix_movies_data (
        show_id varchar(1000),
        type varchar(100),
        title varchar(500),
        director varchar(1000),
        "cast" varchar(1000),
        country varchar(500),
        date_added varchar(100),
        release_year varchar(100),
        rating varchar(100),
        duration varchar(1000),
        listed_in varchar(1000),
        description varchar(1000)
    )
""")

stagging_imdb_movies_data_table_create = ("""
    CREATE TABLE if not exists stagging_imdb_movies_data (
        tconst varchar(100), 
        titleType varchar(1000), 
        primaryTitle varchar(1000), 
        originalTitle varchar(1000), 
        isAdult INT,
        startYear VARCHAR(100), 
        endYear VARCHAR(100), 
        runtimeMinutes varchar(100), 
        genres varchar(1000)
    )
""")

stagging_imdb_movies_rating_data_table_create = ("""
    CREATE TABLE if not exists stagging_imdb_movies_rating_data (
        tconst varchar(100),
        averageRating FLOAT,
        numVotes INT
    )
""")

dim_type_table_create = ("""
    CREATE TABLE if not exists dim_type (
        type_id INT IDENTITY(0,1) PRIMARY KEY NOT NULL,
        type_name varchar(1000)
    )
""")

dim_category_table_create = ("""
    CREATE TABLE if not exists dim_category (
        category_id INT IDENTITY(0,1) PRIMARY KEY NOT NULL,
        category_name varchar(1000)
    )
""")


dim_country_table_create = ("""
    CREATE TABLE if not exists dim_country (
        country_id INT IDENTITY(0,1) PRIMARY KEY NOT NULL,
        country_name varchar(1000)
    )
""")

dim_director_table_create = ("""
    CREATE TABLE if not exists dim_director (
        director_id INT IDENTITY(0,1) PRIMARY KEY NOT NULL,
        director_name varchar(1000)
    )
""")

dim_actor_table_create = ("""
    CREATE TABLE if not exists dim_actor (
        actor_id INT IDENTITY(0,1) PRIMARY KEY NOT NULL,
        actor_name varchar(1000)
    )
""")

dim_movie_title_table_create = ("""
    CREATE TABLE if not exists dim_movie_title (
        movie_id INT IDENTITY(0,1) PRIMARY KEY NOT NULL,
        movie_name varchar(1000),
        movie_type varchar(100),
        movie_release_year varchar(100),
        movie_imdb_rating FLOAT
    )
""")

dim_date_table_create = ("""
    CREATE TABLE if not exists dim_date (
        ts TIMESTAMP NOT NULL, 
        hour INT NOT NULL,
        day INT NOT NULL,
        week INT NOT NULL, 
        month INT NOT NULL,
        year INT NOT NULL,
        weekday INT NOT NULL
    )
""")

fact_movies_added_on_netflix_table_create = ("""
    CREATE TABLE if not exists fact_movies_added_on_netflix (
        fact_id INT IDENTITY(0,1) PRIMARY KEY NOT NULL,
        ts timestamp,
        movie_id INT,
        type_id INT
    )
""")

fact_movies_direction_on_netflix_table_create = ("""
    CREATE TABLE if not exists fact_movies_direction_on_netflix (
        fact_id INT IDENTITY(0,1) PRIMARY KEY NOT NULL,
        ts timestamp,
        movie_id INT,
        type_id INT,
        director_id INT
    )
""")

fact_movies_actor_on_netflix_table_create = ("""
    CREATE TABLE if not exists fact_movies_actor_on_netflix (
        fact_id INT IDENTITY(0,1) PRIMARY KEY NOT NULL,
        ts timestamp,
        movie_id INT,
        type_id INT,
        actor_id INT
    )
""")

fact_movies_country_on_netflix_table_create = ("""
    CREATE TABLE if not exists fact_movies_country_on_netflix (
        fact_id INT IDENTITY(0,1) PRIMARY KEY NOT NULL,
        ts timestamp,
        movie_id INT,
        type_id INT,
        country_id INT
    )
""")

fact_movies_category_on_netflix_table_create = ("""
    CREATE TABLE if not exists fact_movies_category_on_netflix (
        fact_id INT IDENTITY(0,1) PRIMARY KEY NOT NULL,
        ts timestamp,
        movie_id INT,
        type_id INT,
        category_id INT
    )
""")

# STAGING TABLES

staging_netflix_movies_copy = ("""
    copy stagging_netflix_movies_data from {}
    credentials 'aws_iam_role={}' 
    region 'us-west-2'
    CSV
    IGNOREHEADER 1
""").format(NETFLIX_MOVIES_DATA, ARN)

staging_imdb_movies_copy = ("""
    copy stagging_imdb_movies_data from {}
    credentials 'aws_iam_role={}' 
    region 'us-west-2'
    delimiter '\t'
    GZIP
    IGNOREHEADER 1
    ESCAPE
""").format(IMDB_MOVIES_DATA, ARN)

staging_imdb_movies_rating_copy = ("""
    copy stagging_imdb_movies_rating_data from {}
    credentials 'aws_iam_role={}' 
    region 'us-west-2'
    CSV
    delimiter '\t'
    GZIP
    IGNOREHEADER 1
""").format(IMDB_MOVIES_RATING_DATA, ARN)


stagging_imdb_movies_data_table_drop = "DROP TABLE IF EXISTS stagging_imdb_movies_data"
stagging_imdb_movies_rating_data_table_drop = "DROP TABLE IF EXISTS stagging_imdb_movies_rating_data"

# FINAL TABLES

dim_type_table_insert = ("""
    INSERT INTO dim_type (type_name) 
    SELECT distinct type
    FROM stagging_netflix_movies_data
""")

dim_country_table_insert = ("""
    WITH recursive tmp (show_id, idx, country) as 
    (
        select show_id,
            1 as idx,
            split_part(country, ',', 1) as country
        from stagging_netflix_movies_data
        union all
        select d.show_id,
            idx + 1 as idx,
            split_part(d.country, ',', idx + 1) as country
        from stagging_netflix_movies_data d
            join tmp on d.show_id = tmp.show_id
        where idx < REGEXP_COUNT(d.country, ',') + 1
    )
    SELECT DISTINCT TRIM(country) into #temp1
    FROM tmp; 

    INSERT INTO dim_country (country_name) 
    select * from #temp1;
    DROP TABLE #temp1;
""")

dim_movie_table_insert = ("""
    INSERT INTO dim_movie_title (movie_name, movie_type, movie_release_year) 
    SELECT distinct title, type, release_year
    FROM stagging_netflix_movies_data
""")

dim_date_table_insert = ("""
    INSERT INTO dim_date (ts, hour, day, week, month, year, weekday) 
    SELECT DISTINCT
        To_Timestamp(date_added, 'Month DD, YYYY'), 
        extract(hour from To_Timestamp(date_added, 'Month DD, YYYY')), 
        extract(day from To_Timestamp(date_added, 'Month DD, YYYY')), 
        extract(week from To_Timestamp(date_added, 'Month DD, YYYY')),  
        extract(month from To_Timestamp(date_added, 'Month DD, YYYY')),  
        extract(year from To_Timestamp(date_added, 'Month DD, YYYY')),  
        extract(dow from To_Timestamp(date_added, 'Month DD, YYYY'))  
    FROM stagging_netflix_movies_data
""")

dim_director_table_insert= ("""
    WITH recursive tmp (show_id,   idx, director) as 
    (
        select show_id,
            1 as idx,
            split_part(director, ',', 1) as director
        from stagging_netflix_movies_data
        union all
        select d.show_id,
            idx + 1 as idx,
            split_part(d.director, ',', idx + 1) as director
        from stagging_netflix_movies_data d
            join tmp on d.show_id = tmp.show_id
        where idx < REGEXP_COUNT(d.director, ',') + 1
    )
    SELECT DISTINCT TRIM(director) into #temp1
    FROM tmp; 

    INSERT INTO dim_director (director_name)
    select * from #temp1;
    DROP TABLE #temp1;
""")

dim_actor_table_insert= ("""
    with recursive tmp (show_id, idx, actor) as 
    (
        select show_id,
            1 as idx,
            split_part("cast", ',', 1) as actor
        from stagging_netflix_movies_data
        union all
        select d.show_id,
            idx + 1 as idx,
            split_part(d."cast", ',', idx + 1) as actor
        from stagging_netflix_movies_data d
            join tmp on d.show_id = tmp.show_id
        where idx < REGEXP_COUNT(d."cast", ',') + 1
    )
    SELECT DISTINCT TRIM(actor) into #temp1
    FROM tmp; 

    INSERT INTO dim_actor (actor_name)
    select * from #temp1;
    DROP TABLE #temp1;
""")

dim_category_table_insert= ("""
    with recursive tmp (show_id, idx, category) as 
    (
        select show_id,
            1 as idx,
            split_part(listed_in, ',', 1) as category
        from stagging_netflix_movies_data
        union all
        select d.show_id,
            idx + 1 as idx,
            split_part(d.listed_in, ',', idx + 1) as category
        from stagging_netflix_movies_data d
            join tmp on d.show_id = tmp.show_id
        where idx < REGEXP_COUNT(d.listed_in, ',') + 1
    )
    SELECT DISTINCT TRIM(category) into #temp1
    FROM tmp; 

    INSERT INTO dim_category (category_name)
    select * from #temp1;
    DROP TABLE #temp1;
""")
fact_movies_added_on_netflix_table_insert= ("""
    INSERT INTO fact_movies_added_on_netflix (ts, type_id, movie_id) 
    SELECT DISTINCT
        To_Timestamp(date_added, 'Month DD, YYYY') as ts, 
        (select type_id from dim_type where type_name = d.type) as type_id,
        (select movie_id from dim_movie_title where movie_name = d.title and movie_type = d.type and movie_release_year = d.release_year) as movie_id
    FROM stagging_netflix_movies_data d
""")

fact_movies_actor_on_netflix_table_insert= ("""
    with recursive tmp (show_id, ts, type, title,  idx, actor, release_year) as 
    (
        select show_id,
                To_Timestamp(date_added, 'Month DD, YYYY') as ts,
                type,
                title,
            1 as idx,
            split_part("cast", ',', 1) as actor,
                release_year
        from stagging_netflix_movies_data
        union all
        select d.show_id,
                To_Timestamp(date_added, 'Month DD, YYYY') as ts,
                d.type,
                d.title,
            idx + 1 as idx,
            split_part(d."cast", ',', idx + 1) as actor,
                d.release_year
        from stagging_netflix_movies_data d
            join tmp on d.show_id = tmp.show_id
        where idx < REGEXP_COUNT(d."cast", ',') + 1
    )
    select 
        ts, 
        (select movie_id from dim_movie_title where movie_name = d.title and movie_type = d.type and movie_release_year = d.release_year limit 1) as movie_id,
        (select type_id from dim_type where type = d.type limit 1) as type_id, 
        (select actor_id from dim_actor where actor_name = TRIM(d.actor) limit 1) as actor_id
    into #temp1
    from tmp d;

    INSERT INTO fact_movies_actor_on_netflix (ts, movie_id, type_id, actor_id)
    select * from #temp1;
    DROP TABLE #temp1;
""")

fact_movies_direction_on_netflix_table_insert= ("""
    with recursive tmp (show_id, ts, type, title,  idx, director, release_year) as 
    (
        select show_id,
                To_Timestamp(date_added, 'Month DD, YYYY') as ts,
                type,
                title,
            1 as idx,
            split_part(director, ',', 1) as director,
                release_year
        from stagging_netflix_movies_data
        union all
        select d.show_id,
                To_Timestamp(date_added, 'Month DD, YYYY') as ts,
                d.type,
                d.title,
            idx + 1 as idx,
            split_part(d.director, ',', idx + 1) as director,
                d.release_year
        from stagging_netflix_movies_data d
            join tmp on d.show_id = tmp.show_id
        where idx < REGEXP_COUNT(d.director, ',') + 1
    )
    select 
        ts, 
        (select movie_id from dim_movie_title where movie_name = d.title and movie_type = d.type and movie_release_year = d.release_year limit 1) as movie_id,
        (select type_id from dim_type where type = d.type limit 1) as type_id, 
        (select director_id from dim_director where director_name = TRIM(d.director) limit 1) as director_id
    into #temp1
    from tmp d;

    INSERT INTO fact_movies_direction_on_netflix (ts, movie_id, type_id, director_id)
    select * from #temp1;
    DROP TABLE #temp1;
""")

fact_movies_country_on_netflix_table_insert= ("""
    with recursive tmp (show_id, ts, type, title,  idx, country, release_year) as 
    (
        select show_id,
                To_Timestamp(date_added, 'Month DD, YYYY') as ts,
                type,
                title,
            1 as idx,
            split_part(country, ',', 1) as country,
                release_year
        from stagging_netflix_movies_data
        union all
        select d.show_id,
                To_Timestamp(date_added, 'Month DD, YYYY') as ts,
                d.type,
                d.title,
            idx + 1 as idx,
            split_part(d.country, ',', idx + 1) as country,
                d.release_year
        from stagging_netflix_movies_data d
            join tmp on d.show_id = tmp.show_id
        where idx < REGEXP_COUNT(d.country, ',') + 1
    )
    select 
        ts, 
        (select movie_id from dim_movie_title where movie_name = d.title and movie_type = d.type and movie_release_year = d.release_year limit 1) as movie_id,
        (select type_id from dim_type where type = d.type limit 1) as type_id, 
        (select country_id from dim_country where country_name = TRIM(d.country) limit 1) as country_id
    into #temp1
    from tmp d;

    INSERT INTO fact_movies_country_on_netflix (ts, movie_id, type_id, country_id)
    select * from #temp1;
    DROP TABLE #temp1;
""")

fact_movies_category_on_netflix_table_insert= ("""
    with recursive tmp (show_id, ts, type, title,  idx, category, release_year) as 
    (
        select show_id,
                To_Timestamp(date_added, 'Month DD, YYYY') as ts,
                type,
                title,
            1 as idx,
            split_part(listed_in, ',', 1) as category,
                release_year
        from stagging_netflix_movies_data
        union all
        select d.show_id,
                To_Timestamp(date_added, 'Month DD, YYYY') as ts,
                d.type,
                d.title,
            idx + 1 as idx,
            split_part(d.listed_in, ',', idx + 1) as category,
                d.release_year
        from stagging_netflix_movies_data d
            join tmp on d.show_id = tmp.show_id
        where idx < REGEXP_COUNT(d.listed_in, ',') + 1
    )
    select 
        ts, 
        (select movie_id from dim_movie_title where movie_name = d.title and movie_type = d.type and movie_release_year = d.release_year limit 1) as movie_id,
        (select type_id from dim_type where type = d.type limit 1) as type_id, 
        (select category_id from dim_category where category_name = TRIM(d.category) limit 1) as category_id
    into #temp1
    from tmp d;

    INSERT INTO fact_movies_category_on_netflix (ts, movie_id, type_id, category_id)
    select * from #temp1;
    DROP TABLE #temp1;
""")

imdb_data_cleansing = ("""

    delete from stagging_imdb_movies_data 
    where startyear < 2000 
    or titletype not in ('movie', 'tvSeries');
    
    update stagging_imdb_movies_data
    set titletype = 'Movie'
    where titletype = 'movie';

    update stagging_imdb_movies_data
    set titletype = 'TV Show'
    where titletype = 'tvSeries';

    update dim_movie_title
    set movie_imdb_rating = b.averagerating
    from (
        select primarytitle, averagerating
        from (
            select 
            row_number() over (partition by i.primarytitle order by i.primarytitle) as r_num, 
            i.primarytitle, rd.averagerating 
            from stagging_imdb_movies_data i
            inner join 
            stagging_netflix_movies_data n
            on i.startyear = n.release_year
            and i.primarytitle ilike n.title 
            inner join stagging_imdb_movies_rating_data rd
            on rd.tconst = i.tconst
        )a 
        where r_num = 1
    )b
    inner join dim_movie_title t 
    on b.primarytitle = t.movie_name;
""")

dim_movies_duplication_check = ("""
    select count(*) 
    from (
        select             
        row_number() over (partition by movie_name order by movie_name) as r_num
        from dim_movie_title
    )a
    where a.r_num > 1
""")

dim_movies_null_check = ("""
    select count(*) from dim_movie_title
    where movie_name is null
""")

# QUERY LISTS
create_table_queries = [
    stagging_netflix_movies_data_table_create,
    stagging_imdb_movies_data_table_create,
    stagging_imdb_movies_rating_data_table_create,
    dim_type_table_create, 
    dim_country_table_create, 
    dim_date_table_create, 
    dim_director_table_create, 
    dim_actor_table_create,
    dim_movie_title_table_create,
    dim_category_table_create,
    fact_movies_added_on_netflix_table_create,
    fact_movies_direction_on_netflix_table_create,
    fact_movies_actor_on_netflix_table_create,
    fact_movies_country_on_netflix_table_create,
    fact_movies_category_on_netflix_table_create,
    ]
drop_table_queries = [
    stagging_netflix_movies_data_table_drop, 
    stagging_imdb_movies_data_table_drop,
    stagging_imdb_movies_rating_data_table_drop,
    dim_type_table_drop, 
    dim_country_table_drop, 
    dim_date_table_drop, 
    dim_director_table_drop, 
    dim_actor_table_drop,
    dim_movie_title_table_drop,
    dim_category_table_drop,
    fact_movies_added_on_table_drop,
    fact_movies_direction_on_netflix_table_drop,
    fact_movies_actor_on_netflix_table_drop,
    fact_movies_country_on_netflix_table_drop,
    fact_movies_category_on_netflix_table_drop,
]

copy_table_queries = [
    staging_netflix_movies_copy,
    staging_imdb_movies_copy,
    staging_imdb_movies_rating_copy
]

insert_table_queries = [
    dim_type_table_insert, 
    dim_date_table_insert,
    dim_movie_table_insert,
    dim_country_table_insert,
    dim_director_table_insert,
    dim_actor_table_insert,
    dim_category_table_insert,
    fact_movies_actor_on_netflix_table_insert,
    fact_movies_direction_on_netflix_table_insert,
    fact_movies_added_on_netflix_table_insert,
    fact_movies_country_on_netflix_table_insert,
    fact_movies_category_on_netflix_table_insert
]

data_cleansing_queries = [
    imdb_data_cleansing
]

data_quality_check_queries = [
    dim_movies_duplication_check,
    dim_movies_null_check
]

