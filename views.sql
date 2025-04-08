--export joined parquet
COPY(
    SELECT * FROM './processed_data/full_join.parquet'
) TO './final_csv/full_join.csv' WITH (HEADER, DELIMITER ',');

-- Calculating daily average metrics
COPY (
    SELECT strftime(tpep_pickup_datetime, '%Y-%m-%d') AS day,
           SUM(fare_amount) AS total_fare,
           AVG(trip_distance) AS avg_distance,
           SUM(trip_distance) AS sum_distance,
           AVG(Tip_amount) as avg_tip,
           SUM(Tip_amount) as sum_tip
    FROM './processed_data/full_join.parquet'
    GROUP BY day
    ORDER BY day ASC
) TO './final_csv/daily_revenue.csv' WITH (HEADER, DELIMITER ',');

-- Calcularing daily trips by pick up and drop off boroughs
COPY (
    SELECT * 
    FROM './processed_data/daily_trips_borough.parquet'
    WHERE Pickup_Borough != 'Unknown' and Dropoff_Borough != 'Unknown'
) TO './final_csv/daily_trips_borough.csv' WITH (HEADER, DELIMITER ',');

-- Caculating daily trips by pick up and drop off zones
COPY (
    SELECT * 
    FROM './processed_data/daily_trips_zone.parquet'
    WHERE Pickup_Zone != 'Unknown' and Dropoff_Zone != 'Unknown'
) TO './final_csv/daily_trips_zone.csv' WITH (HEADER, DELIMITER ',');

-- pivot covid data join with pick up and drop off locations
COPY (
    WITH covid_long AS (
        SELECT 
            date_of_interest,
            'Manhattan' AS Borough,
            MN_CASE_COUNT,
            MN_PROBABLE_CASE_COUNT,
            MN_HOSPITALIZED_COUNT,
            MN_DEATH_COUNT
        FROM './raw_data/covid_data/data_by_day.csv'
        UNION ALL
        SELECT 
            date_of_interest,
            'Brooklyn',
            BK_CASE_COUNT,
            BK_PROBABLE_CASE_COUNT,
            BK_HOSPITALIZED_COUNT,
            BK_DEATH_COUNT
        FROM './raw_data/covid_data/data_by_day.csv'
        UNION ALL
        SELECT 
            date_of_interest,
            'Queens',
            QN_CASE_COUNT,
            QN_PROBABLE_CASE_COUNT,
            QN_HOSPITALIZED_COUNT,
            QN_DEATH_COUNT
        FROM './raw_data/covid_data/data_by_day.csv'
        UNION ALL
        SELECT 
            date_of_interest,
            'Bronx',
            BX_CASE_COUNT,
            BX_PROBABLE_CASE_COUNT,
            BX_HOSPITALIZED_COUNT,
            BX_DEATH_COUNT
        FROM './raw_data/covid_data/data_by_day.csv'
        UNION ALL
        SELECT 
            date_of_interest,
            'Staten Island',
            SI_CASE_COUNT,
            SI_PROBABLE_CASE_COUNT,
            SI_HOSPITALIZED_COUNT,
            SI_DEATH_COUNT
        FROM './raw_data/covid_data/data_by_day.csv'
    )
    SELECT 
        t.Pickup_Borough AS Pickup_Borough,
        t.Dropoff_Borough AS Dropoff_Borough,
        t.Pickup_Date AS Pickup_date,
        c.MN_CASE_COUNT AS Pickup_Cases,
        c.MN_DEATH_COUNT AS Pickup_Deaths,
        c.MN_PROBABLE_CASE_COUNT AS Pickup_probable_cases,
        c.MN_HOSPITALIZED_COUNT AS Pickup_hospital_cases,
        d.MN_CASE_COUNT AS Dropoff_Cases,
        d.MN_DEATH_COUNT AS Dropoff_Deaths,
        d.MN_PROBABLE_CASE_COUNT AS Dropoff_probable_cases,
        d.MN_HOSPITALIZED_COUNT AS Dropoff_hospital_cases
    FROM './processed_data/daily_trips_borough.parquet' t
    LEFT JOIN covid_long c 
        ON t.Pickup_Borough = c.Borough 
        AND t.Pickup_Date = c.date_of_interest
    LEFT JOIN covid_long d 
        ON t.Dropoff_Borough = d.Borough 
        AND t.Pickup_Date = d.date_of_interest
) TO './final_csv/trip_zone_covid.csv' WITH (HEADER, DELIMITER ',');

-- airport trips + case count by day
COPY (
    WITH airport_trips AS (
        SELECT 
            pickup_date,
            Dropoff_Zone AS airport_zone,
            'to_airport' AS direction,
            SUM(trip_count) AS total_trips
        FROM './processed_data/daily_trips_zone.parquet'
        WHERE Dropoff_Zone LIKE '%Airport%'
        GROUP BY pickup_date, Dropoff_Zone

        UNION ALL

        SELECT 
            pickup_date,
            Pickup_Zone AS airport_zone,
            'from_airport' AS direction,
            SUM(trip_count) AS total_trips
        FROM './processed_data/daily_trips_zone.parquet'
        WHERE Pickup_Zone LIKE '%Airport%'
        GROUP BY pickup_date, Pickup_Zone
    )

    SELECT 
        at.pickup_date, 
        at.airport_zone, 
        at.direction, 
        at.total_trips, 
        covid.CASE_COUNT, 
        covid.PROBABLE_CASE_COUNT, 
        covid.HOSPITALIZED_COUNT,
        covid.DEATH_COUNT,
    FROM airport_trips at
    LEFT JOIN './raw_data/covid_data/data_by_day.csv' covid
    ON at.pickup_date = covid.date_of_interest
    ORDER BY at.pickup_date, at.airport_zone, at.direction
) TO './final_csv/trip_airport_covid.csv' WITH (HEADER, DELIMITER ',');

-- payment type + covid
COPY (
    SELECT 
        strftime(tpep_pickup_datetime, '%Y-%m-%d') AS day,
        CASE 
            WHEN payment_type = 1 THEN 'Credit Card'
            WHEN payment_type = 2 THEN 'Cash'
        END AS payment_method,
        COUNT(*) AS count,
        covid.CASE_COUNT, 
        covid.PROBABLE_CASE_COUNT, 
        covid.HOSPITALIZED_COUNT,
        covid.DEATH_COUNT
    FROM './processed_data/full_join.parquet' AS trips
    LEFT JOIN './raw_data/covid_data/data_by_day.csv' AS covid
    ON strftime(trips.tpep_pickup_datetime, '%Y-%m-%d') = covid.date_of_interest
    WHERE payment_type IN (1, 2)
    GROUP BY day, payment_method, covid.CASE_COUNT, covid.PROBABLE_CASE_COUNT, covid.HOSPITALIZED_COUNT, covid.DEATH_COUNT
    ORDER BY day, payment_method
) TO './final_csv/trip_payment_covid.csv' WITH (HEADER, DELIMITER ',');