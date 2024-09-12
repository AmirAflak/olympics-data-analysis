
# Olympics Data Analysis

## Project Overview

This project implements an ETL pipeline using **Apache Spark** to process Olympic athlete data. After loading the data into a PostgreSQL database, a series of SQL analysis queries were performed to generate insights into athlete performances, country-wise statistics, and other key metrics.


## Analysis Section

### Total Medal Count by Country (NOC)
```sql
WITH medals AS (
    SELECT *
    FROM db.public.results
    WHERE medal IS NOT NULL
      AND event NOT LIKE '%(YOG)'
),
medals_filtered AS (
    SELECT DISTINCT year, type, discipline, noc, event, medal
    FROM medals
)
SELECT noc, COUNT(medal) AS total_medals
FROM medals_filtered
GROUP BY noc
ORDER BY total_medals DESC
LIMIT 5;
```




    ┌─────────┬──────────────┐
    │   noc   │ total_medals │
    │ varchar │    int64     │
    ├─────────┼──────────────┤
    │ USA     │         2997 │
    │ URS     │         1197 │
    │ GER     │         1078 │
    │ GBR     │          989 │
    │ FRA     │          939 │
    └─────────┴──────────────┘



### Count of Athletes by Country


```sql
SELECT born_country, COUNT(*) AS athlete_count
FROM db.public.bios
GROUP BY born_country
ORDER BY athlete_count DESC
LIMIT 5;
```




    ┌──────────────┬───────────────┐
    │ born_country │ athlete_count │
    │   varchar    │     int64     │
    ├──────────────┼───────────────┤
    │              │         32864 │
    │ USA          │          9641 │
    │ GER          │          6891 │
    │ GBR          │          5792 │
    │ FRA          │          5143 │
    └──────────────┴───────────────┘




### Top Performing Athletes by Medal Count


```sql
SELECT b.name, COUNT(r.medal) AS medal_count
FROM db.public.results r
JOIN db.public.bios b ON r.athlete_id = b.athlete_id
WHERE r.medal IS NOT NULL
GROUP BY r.athlete_id, b.name
ORDER BY medal_count DESC
LIMIT 10;
```




    ┌──────────────────────┬─────────────┐
    │         name         │ medal_count │
    │       varchar        │    int64    │
    ├──────────────────────┼─────────────┤
    │ Michael Phelps       │          28 │
    │ Larisa Latynina      │          18 │
    │ Emma McKeon          │          17 │
    │ Marit Bjørgen        │          15 │
    │ Nikolay Andrianov    │          15 │
    │ Takashi Ono          │          13 │
    │ Ireen Wüst           │          13 │
    │ Ole Einar Bjørndalen │          13 │
    │ Edoardo Mangiarotti  │          13 │
    │ Boris Shakhlin       │          13 │
    ├──────────────────────┴─────────────┤
    │ 10 rows                  2 columns │
    └────────────────────────────────────┘



### Average Height and Weight of Athletes by Country:


```sql
WITH unique_participation AS (
    SELECT DISTINCT year, type, discipline, noc, event, athlete_id
    FROM db.public.results
    WHERE event NOT LIKE '%(YOG)'
)
SELECT born_country, 
       AVG(height_cm) AS avg_height, 
       AVG(weight_kg) AS avg_weight
FROM db.public.bios b
JOIN unique_participation r ON b.athlete_id = r.athlete_id
WHERE height_cm IS NOT NULL 
  AND weight_kg IS NOT NULL
GROUP BY born_country
ORDER BY avg_height DESC
LIMIT 5;
```




    ┌──────────────┬────────────────────┬────────────┐
    │ born_country │     avg_height     │ avg_weight │
    │   varchar    │       double       │   double   │
    ├──────────────┼────────────────────┼────────────┤
    │ Milde        │ 195.66666666666666 │      107.0 │
    │ Prignitz     │              194.0 │       90.0 │
    │ GIB          │              191.0 │       89.0 │
    │ ANT          │              188.0 │       80.0 │
    │ AGU          │              186.0 │       78.0 │
    └──────────────┴────────────────────┴────────────┘

### Medal Distribution by Discipline

```sql
WITH medals AS (
    SELECT *
    FROM db.public.results
    WHERE medal IS NOT NULL
      AND event NOT LIKE '%(YOG)'
),
medals_filtered AS (
    SELECT DISTINCT year, type, discipline, noc, event, medal
    FROM medals
)
SELECT discipline, COUNT(medal) AS total_medals
FROM medals_filtered
GROUP BY discipline
ORDER BY total_medals DESC;
```




    ┌──────────────────────────────────┬──────────────┐
    │            discipline            │ total_medals │
    │             varchar              │    int64     │
    ├──────────────────────────────────┼──────────────┤
    │ Athletics                        │         3177 │
    │ Swimming (Aquatics)              │         1785 │
    │ Wrestling                        │         1359 │
    │ Artistic Gymnastics (Gymnastics) │         1009 │
    │ Boxing                           │          997 │
    │ Shooting                         │          895 │
    │ Rowing                           │          825 │
    │ Fencing                          │          709 │
    │ Weightlifting                    │          666 │
    │ Speed Skating (Skating)          │          607 │
    │            ·                     │            · │
    │            ·                     │            · │
    │            ·                     │            · │
    │ Cycling BMX Freestyle (Cycling)  │            6 │
    │ Racquets                         │            5 │
    │ Lacrosse                         │            5 │
    │ Roque                            │            3 │
    │ Equestrian Driving (Equestrian)  │            3 │
    │ Motorboating                     │            3 │
    │ Military Ski Patrol (Skiing)     │            3 │
    │ Art Competitions                 │            2 │
    │ Cricket                          │            2 │
    │ Basque pelota                    │            1 │
    ├──────────────────────────────────┴──────────────┤
    │ 80 rows (20 shown)                    2 columns │
    └─────────────────────────────────────────────────┘


### Countries with the Most Olympic Participation


```sql
WITH unique_participation AS (
    SELECT DISTINCT year, type, discipline, noc, event, athlete_id
    FROM db.public.results
    WHERE event NOT LIKE '%(YOG)'
)
SELECT b.noc, COUNT(DISTINCT b.athlete_id) AS athlete_count
FROM db.public.bios b
JOIN unique_participation r ON b.athlete_id = r.athlete_id
GROUP BY b.noc
ORDER BY athlete_count DESC;
```




    ┌────────────────────────────┬───────────────┐
    │            NOC             │ athlete_count │
    │          varchar           │     int64     │
    ├────────────────────────────┼───────────────┤
    │ United States              │          9921 │
    │ Great Britain              │          6395 │
    │ France                     │          6291 │
    │ Canada                     │          5203 │
    │ Italy                      │          5131 │
    │ Germany                    │          4640 │
    │ Japan                      │          4548 │
    │ Australia                  │          4101 │
    │ Sweden                     │          3875 │
    │ People's Republic of China │          3081 │
    │        ·                   │             · │
    │        ·                   │             · │
    │        ·                   │             · │
    │ Argentina Spain            │             1 │
    │ France Hungary             │             1 │
    │ Great Britain South Africa │             1 │
    │ Turkmenistan Türkiye       │             1 │
    │ Cuba Mexico                │             1 │
    │ Andorra Spain              │             1 │
    │ Mozambique South Africa    │             1 │
    │ Canada Russian Federation  │             1 │
    │ Australia Croatia          │             1 │
    │ Australia Belgium          │             1 │
    ├────────────────────────────┴───────────────┤
    │ 696 rows (20 shown)              2 columns │
    └────────────────────────────────────────────┘



### Athlete Participation Over the Years



```sql
WITH unique_participation AS (
    SELECT DISTINCT year, type, discipline, noc, event, athlete_id
    FROM db.public.results
    WHERE event NOT LIKE '%(YOG)'
)
SELECT year, COUNT(DISTINCT athlete_id) AS athlete_count
FROM unique_participation
GROUP BY year
ORDER BY year ASC;
```




    ┌───────┬───────────────┐
    │ year  │ athlete_count │
    │ int32 │     int64     │
    ├───────┼───────────────┤
    │  1896 │           182 │
    │  1900 │          1234 │
    │  1904 │           667 │
    │  1908 │          2112 │
    │  1912 │          2447 │
    │  1920 │          2689 │
    │  1924 │          3494 │
    │  1928 │          3427 │
    │  1932 │          1627 │
    │  1936 │          4661 │
    │    ·  │            ·  │
    │    ·  │            ·  │
    │    ·  │            ·  │
    │  2006 │          2505 │
    │  2008 │         10956 │
    │  2010 │          2537 │
    │  2012 │         10572 │
    │  2014 │          2765 │
    │  2016 │         11210 │
    │  2018 │          2799 │
    │  2020 │         11125 │
    │  2022 │          2470 │
    │  NULL │          1026 │
    ├───────┴───────────────┤
    │  38 rows (20 shown)   │
    └───────────────────────┘





## Installation Guide

To run this project, follow the steps below:

1. **Clone the repository**:
   ```bash
   git clone https://github.com/amiraflak/olympics-data-analysis
   cd olympics-data-analysis
   ```

2. **Run Docker Compose**: This step sets up the PostgreSQL database where the data will be loaded.
   ```bash
   docker-compose up -d
   ```

3. **Load Data with Spark Scala**:
   - Navigate to the `src/main/scala` directory.
   - Run the `Main.scala` file to execute the ETL pipeline:
     ```bash
     scala Main.scala
     ```

4. **Run SQL Queries**:
   - Once the data is loaded into PostgreSQL, you can connect to the database and run the analysis queries provided in the analysis section.



## Contribution

Contributions are welcome! Feel free to open an issue or submit a pull request to improve the project.

## References

- Dataset: [Olympic Athlete Data](https://github.com/KeithGalli/Olympics-Dataset)
