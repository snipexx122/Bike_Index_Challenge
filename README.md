# Bike_Index_Challenge

The project purpose is to ingest data daily from bike index api.


## Tech Stack
**Databse:** Postgress

**Containerization:** Docker

**Program:** python

**framework:** spark

**schedueler:** airflow

## Steps

1. ingest data from api based on the year you want to start with(start early for initial load)
2. process data in spark to make sure the new data is unique and that no duplication records exist.
3. Insert it into the postgresql database.(that is for simplification purposes)

## database table statement
date_stolen | description | frame_colors | frame_model | id | is_stock_img | large_img | location_found | manufacturer_name | external_id | registry_name | registry_url | serial | status | stolen | stolen_coordinates | stolen_location | thumb | title | url | year
| :--- | ---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: 
bigint  | text | text[] | text | integer | boolean | text | text | text | text | text | text | text | text | text | boolean | double precision[] | text | text | text | text | integer

```javascript
        CREATE TABLE bike_data (
  date_stolen bigint,
  description text,
  frame_colors text[],
  frame_model text,
  id integer,
  is_stock_img boolean,
  large_img text,
  location_found text,
  manufacturer_name text,
  external_id text,
  registry_name text,
  registry_url text,
  serial text,
  status text,
  stolen boolean,
  stolen_coordinates double precision[],
  stolen_location text,
  thumb text,
  title text,
  url text,
  year integer
);
```

## Run Locally

1. Clone the project

```bash
  git clone https://github.com/snipexx122/event_web_crawler.git
```



2. Go to the project directory

```bash
  cd BIKE_INDEX_CHALLENGE/docker
```

3. Run 

```bash
  docker-compose up 
```

4. wait till spark-master container is running then

```bash
  docker exec -it spark-master /bin/bash
```

5. then isnide spark master run 
 
```bash
  pip install requests
```

6. after that open link http://localhost:8282/admin/

**user:** airflow

**password:** airflow

7. open admin -> connections -> spark_default and change

    **host:** spark://spark
    **port:** 7077

8. 
```bash
  docker exec -it docker-postgres-1 /bin/bash
```
9. 
```bash
  psql -U test
```

and excute the create table statement above.

10. excute bike_etl dag.

11. 
```bash
  docker exec -it docker-postgres-1 /bin/bash
```

12. 
```bash
  psql -U test
```

13. execute the query.
```javascript
select * from bike_Data;
```

## Note

The code runs daily to get data say of 2023 then it makes sure that no duplicate data that already exists in the database is entered. data that will be written are the ones that are new or were upated.







