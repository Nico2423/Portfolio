--Vamos a crear una tabla de estaciones la cuál consiste en la unión de los diferentes registros que tenemos, esto con el fin de limpiarla y usarla para los futuros análisis
SELECT
id,name,latitude,longitude
INTO Cyclistic.dbo.stations
FROM
Cyclistic.dbo.Divvy_Stations_2013

UNION

SELECT
id,name,latitude,longitude
FROM
Cyclistic.dbo.[Divvy_Stations_2014]

UNION

SELECT 
id,name,latitude,longitude
FROM
Cyclistic.dbo.Divvy_Stations_2015

UNION

SELECT
id,name,latitude,longitude
FROM
Cyclistic.dbo.Divvy_Stations_2016

UNION

SELECT
id,name,latitude,longitude
FROM
Cyclistic.dbo.Divvy_Stations_2017




--Modificamos la latitud y longitud para tenerlas a todas con la misma medición, ya que cada una se encuentra multiplicada por un valor diferente, lo cuál hace cada medición inexacta
UPDATE Cyclistic.dbo.stations
SET latitude =
CASE
	WHEN latitude > 1000000000 THEN latitude / 100000000
	WHEN latitude > 100000000 THEN latitude / 10000000
	WHEN latitude > 10000000 THEN latitude / 1000000
	WHEN latitude > 1000000 THEN latitude / 100000
	WHEN latitude > 100000 THEN latitude / 10000
	WHEN latitude > 10000 THEN latitude / 1000
END;


UPDATE Cyclistic.dbo.stations
SET longitude = 
CASE
	WHEN ABS(longitude) > 1000000000 THEN longitude / 100000000
	WHEN ABS(longitude) > 100000000 THEN longitude / 10000000
	WHEN ABS(longitude) > 10000000 THEN longitude / 1000000
	WHEN ABS(longitude) > 1000000 THEN longitude / 100000
	WHEN ABS(longitude) > 100000 THEN longitude / 10000
	WHEN ABS(longitude) > 10000 THEN longitude / 1000
END;



--Al momento de unir la tabla nos dimos cuenta de que muchas estaciones cuentan con varias apariciones con unas coordenadas diferentes, con esto nos aseguramos que haya sólo una 
--aparición por estación
SELECT
	id,
    name,
    AVG(latitude)  AS lat_promedio,
    AVG(longitude) AS long_promedio
INTO Cyclistic.dbo.clean_data_stations
FROM Cyclistic.dbo.stations
GROUP BY id,name;





--Revisamos si hay filas duplicadas en nuestro dataset de viajes 2013
WITH cte AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY
                trip_id,
                starttime,
                stoptime,
                bikeid,
                tripduration,
                from_station_id,
                from_station_name,
                to_station_name,
                to_station_id,
                usertype,
                gender,
                birthday
            ORDER BY trip_id
        ) AS dupl
    FROM Cyclistic.dbo.Divvy_Trips_2013
)
SELECT
    *
FROM cte
WHERE dupl > 1;




--Ahora creamos nuestra tabla de viajes del 2013 donde se incluyan tanto coordenadas como diferentes métricas
SELECT
tr.trip_id,
tr.starttime,
tr.from_station_id,
tr.to_station_id,
ROUND(6371 * 2 *
ASIN(
    SQRT(
        POWER(SIN(RADIANS(ends.lat_promedio - st.lat_promedio) / 2), 2) +
        COS(RADIANS(st.lat_promedio)) *
        COS(RADIANS(ends.lat_promedio)) *
        POWER(SIN(RADIANS(ends.long_promedio -  st.long_promedio) / 2), 2)
    )
),2) AS distance_km,
tr.tripduration AS tripduration_seconds,
tr.usertype
INTO Cyclistic.dbo.trips_2013
FROM
Cyclistic.dbo.Divvy_Trips_2013 AS tr
LEFT JOIN Cyclistic.dbo.clean_data_stations AS st
ON tr.from_station_id = st.id
LEFT JOIN Cyclistic.dbo.clean_data_stations AS ends
ON tr.to_station_id = ends.id


--Recortamos la fecha a tipo date ya que en este punto no necesitamos hora y segundos
ALTER TABLE Cyclistic.dbo.trips_2016
ALTER COLUMN starttime date;






--Vamos a unir todos los registros de 2014 ya que se encuentra dispersados por partes del año, esto para tener toda la información unificada

SELECT 
trip_id,starttime, from_station_id,to_station_id,tripduration,usertype
INTO Cyclistic.dbo.data_trips_2014
FROM 
Cyclistic.dbo.Divvy_Trips_2014_Q1Q2

UNION ALL

SELECT
trip_id,starttime, from_station_id,to_station_id,tripduration,usertype
FROM 
Cyclistic.dbo.[Divvy_Trips_2014-Q3-07]

UNION ALL

SELECT
trip_id,starttime, from_station_id,to_station_id,tripduration,usertype
FROM 
Cyclistic.dbo.[Divvy_Trips_2014-Q3-0809]

UNION ALL

SELECT
trip_id,starttime, from_station_id,to_station_id,tripduration,usertype
FROM 
Cyclistic.dbo.[Divvy_Trips_2014-Q4]



--Creamos nuestra tabla de viajes del 2014

SELECT
tr.trip_id,
tr.starttime,
tr.from_station_id,
tr.to_station_id,
ROUND(6371 * 2 *
ASIN(
    SQRT(
        POWER(SIN(RADIANS(fs.lat_promedio - st.lat_promedio) / 2), 2) +
        COS(RADIANS(st.lat_promedio)) *
        COS(RADIANS(fs.lat_promedio)) *
        POWER(SIN(RADIANS(fs.long_promedio -  st.long_promedio) / 2), 2)
    )
),2) AS distance_km,
tr.tripduration AS tripduration_seconds,
tr.usertype
INTO Cyclistic.dbo.trips_2014
FROM 
Cyclistic.dbo.data_trips_2014 AS tr
LEFT JOIN Cyclistic.dbo.clean_data_stations AS st
ON tr.from_station_id = st.id
LEFT JOIN Cyclistic.dbo.clean_data_stations AS fs
ON tr.to_station_id = fs.id



--2015
SELECT 
trip_id,starttime, from_station_id,to_station_id,tripduration,usertype
INTO Cyclistic.dbo.data_trips_2015
FROM 
Cyclistic.dbo.[Divvy_Trips_2015-Q1]

UNION ALL

SELECT
trip_id,starttime, from_station_id,to_station_id,tripduration,usertype
FROM 
Cyclistic.dbo.[Divvy_Trips_2015-Q2]

UNION ALL

SELECT
trip_id,starttime, from_station_id,to_station_id,tripduration,usertype
FROM 
Cyclistic.dbo.[Divvy_Trips_2015_07]

UNION ALL

SELECT
trip_id,starttime, from_station_id,to_station_id,tripduration,usertype
FROM 
Cyclistic.dbo.[Divvy_Trips_2015_08]

UNION ALL

SELECT
trip_id,starttime, from_station_id,to_station_id,tripduration,usertype
FROM 
Cyclistic.dbo.[Divvy_Trips_2015_09]

UNION ALL

SELECT
trip_id,starttime, from_station_id,to_station_id,tripduration,usertype
FROM 
Cyclistic.dbo.[Divvy_Trips_2015_Q4]



--Creación tabla de viajes 2015
SELECT
tr.trip_id,
tr.starttime,
tr.from_station_id,
tr.to_station_id,
ROUND(6371 * 2 *
ASIN(
    SQRT(
        POWER(SIN(RADIANS(fs.lat_promedio - st.lat_promedio) / 2), 2) +
        COS(RADIANS(st.lat_promedio)) *
        COS(RADIANS(fs.lat_promedio)) *
        POWER(SIN(RADIANS(fs.long_promedio -  st.long_promedio) / 2), 2)
    )
),2) AS distance_km,
tr.tripduration AS tripduration_seconds,
tr.usertype
INTO Cyclistic.dbo.trips_2015
FROM 
Cyclistic.dbo.data_trips_2015 AS tr
LEFT JOIN Cyclistic.dbo.clean_data_stations AS st
ON tr.from_station_id = st.id
LEFT JOIN Cyclistic.dbo.clean_data_stations AS fs
ON tr.to_station_id = fs.id



--2016
SELECT 
trip_id,starttime, from_station_id,to_station_id,tripduration,usertype
INTO Cyclistic.dbo.data_trips_2016
FROM 
Cyclistic.dbo.[Divvy_Trips_2016_04]

UNION ALL

SELECT
trip_id,starttime, from_station_id,to_station_id,tripduration,usertype
FROM 
Cyclistic.dbo.[Divvy_Trips_2016_05]

UNION ALL

SELECT
trip_id,starttime, from_station_id,to_station_id,tripduration,usertype
FROM 
Cyclistic.dbo.[Divvy_Trips_2016_06]

UNION ALL

SELECT
trip_id,starttime, from_station_id,to_station_id,tripduration,usertype
FROM 
Cyclistic.dbo.[Divvy_Trips_2016_Q1]

UNION ALL

SELECT
trip_id,starttime, from_station_id,to_station_id,tripduration,usertype
FROM 
Cyclistic.dbo.[Divvy_Trips_2016_Q3]

UNION ALL

SELECT
trip_id,starttime, from_station_id,to_station_id,tripduration,usertype
FROM 
Cyclistic.dbo.[Divvy_Trips_2016_Q4]



--Creación de tabla de viajes 2016

SELECT
tr.trip_id,
tr.starttime,
tr.from_station_id,
tr.to_station_id,
ROUND(6371 * 2 *
ASIN(
    SQRT(
        POWER(SIN(RADIANS(fs.lat_promedio - st.lat_promedio) / 2), 2) +
        COS(RADIANS(st.lat_promedio)) *
        COS(RADIANS(fs.lat_promedio)) *
        POWER(SIN(RADIANS(fs.long_promedio -  st.long_promedio) / 2), 2)
    )
),2) AS distance_km,
tr.tripduration AS tripduration_seconds,
tr.usertype
INTO Cyclistic.dbo.trips_2016
FROM 
Cyclistic.dbo.data_trips_2016 AS tr
LEFT JOIN Cyclistic.dbo.clean_data_stations AS st
ON tr.from_station_id = st.id
LEFT JOIN Cyclistic.dbo.clean_data_stations AS fs
ON tr.to_station_id = fs.id


--2017
SELECT 
trip_id,start_time, from_station_id,to_station_id,tripduration,usertype
INTO Cyclistic.dbo.data_trips_2017
FROM 
Cyclistic.dbo.[Divvy_Trips_2017_Q1]

UNION ALL

SELECT
trip_id,start_time, from_station_id,to_station_id,tripduration,usertype
FROM 
Cyclistic.dbo.[Divvy_Trips_2017_Q2]

UNION ALL

SELECT
trip_id,start_time, from_station_id,to_station_id,tripduration,usertype
FROM 
Cyclistic.dbo.[Divvy_Trips_2017_Q3]

UNION ALL

SELECT
trip_id,start_time, from_station_id,to_station_id,tripduration,usertype
FROM 
Cyclistic.dbo.[Divvy_Trips_2017_Q4]


--Creación de tabla de viajes 2017
SELECT
tr.trip_id,
tr.start_time,
tr.from_station_id,
tr.to_station_id,
ROUND(6371 * 2 *
ASIN(
    SQRT(
        POWER(SIN(RADIANS(fs.lat_promedio - st.lat_promedio) / 2), 2) +
        COS(RADIANS(st.lat_promedio)) *
        COS(RADIANS(fs.lat_promedio)) *
        POWER(SIN(RADIANS(fs.long_promedio -  st.long_promedio) / 2), 2)
    )
),2) AS distance_km,
tr.tripduration AS tripduration_seconds,
tr.usertype
INTO Cyclistic.dbo.trips_2017
FROM 
Cyclistic.dbo.data_trips_2017 AS tr
LEFT JOIN Cyclistic.dbo.clean_data_stations AS st
ON tr.from_station_id = st.id
LEFT JOIN Cyclistic.dbo.clean_data_stations AS fs
ON tr.to_station_id = fs.id


--2018
SELECT
trip_id,start_time, from_station_id,to_station_id,tripduration,usertype
INTO Cyclistic.dbo.data_trips_2018
FROM 
Cyclistic.dbo.[Divvy_Trips_2018_Q2]

UNION ALL

SELECT
trip_id,start_time, from_station_id,to_station_id,tripduration,usertype
FROM 
Cyclistic.dbo.[Divvy_Trips_2018_Q3]

UNION ALL

SELECT
trip_id,start_time, from_station_id,to_station_id,tripduration,usertype
FROM 
Cyclistic.dbo.[Divvy_Trips_2018_Q4]

UNION ALL

SELECT 
[_01_Rental_Details_Rental_ID],	[_01_Rental_Details_Local_Start_Time],[_03_Rental_Start_Station_ID], [_02_Rental_End_Station_ID],[_01_Rental_Details_Duration_In_Seconds_Uncapped],[User_Type]	

FROM 
Cyclistic.dbo.[Divvy_Trips_2018_Q1]



--Creación de la tabla de viajes del 2018
SELECT
tr.trip_id,
tr.start_time,
tr.from_station_id,
tr.to_station_id,
ROUND(6371 * 2 *
ASIN(
    SQRT(
        POWER(SIN(RADIANS(fs.lat_promedio - st.lat_promedio) / 2), 2) +
        COS(RADIANS(st.lat_promedio)) *
        COS(RADIANS(fs.lat_promedio)) *
        POWER(SIN(RADIANS(fs.long_promedio -  st.long_promedio) / 2), 2)
    )
),2) AS distance_km,
tr.tripduration AS tripduration_seconds,
tr.usertype
INTO Cyclistic.dbo.trips_2018
FROM 
Cyclistic.dbo.data_trips_2018 AS tr
LEFT JOIN Cyclistic.dbo.clean_data_stations AS st
ON tr.from_station_id = st.id
LEFT JOIN Cyclistic.dbo.clean_data_stations AS fs
ON tr.to_station_id = fs.id


--2019

SELECT
trip_id,start_time, from_station_id,to_station_id,tripduration,usertype
INTO Cyclistic.dbo.data_trips_2019
FROM 
Cyclistic.dbo.[Divvy_Trips_2019_Q1]

UNION ALL

SELECT
trip_id,start_time, from_station_id,to_station_id,tripduration,usertype
FROM 
Cyclistic.dbo.[Divvy_Trips_2019_Q3]

UNION ALL

SELECT
trip_id,start_time, from_station_id,to_station_id,tripduration,usertype
FROM 
Cyclistic.dbo.[Divvy_Trips_2019_Q4]

UNION ALL

SELECT 
[_01_Rental_Details_Rental_ID],	[_01_Rental_Details_Local_Start_Time],[_03_Rental_Start_Station_ID], [_02_Rental_End_Station_ID],[_01_Rental_Details_Duration_In_Seconds_Uncapped],[User_Type]	

FROM 
Cyclistic.dbo.[Divvy_Trips_2019_Q2]


--Creación de la tabla de viajes del 2019
SELECT
tr.trip_id,
tr.start_time,
tr.from_station_id,
tr.to_station_id,
ROUND(6371 * 2 *
ASIN(
    SQRT(
        POWER(SIN(RADIANS(fs.lat_promedio - st.lat_promedio) / 2), 2) +
        COS(RADIANS(st.lat_promedio)) *
        COS(RADIANS(fs.lat_promedio)) *
        POWER(SIN(RADIANS(fs.long_promedio -  st.long_promedio) / 2), 2)
    )
),2) AS distance_km,
tr.tripduration AS tripduration_seconds,
tr.usertype
INTO Cyclistic.dbo.trips_2019
FROM 
Cyclistic.dbo.data_trips_2019 AS tr
LEFT JOIN Cyclistic.dbo.clean_data_stations AS st
ON tr.from_station_id = st.id
LEFT JOIN Cyclistic.dbo.clean_data_stations AS fs
ON tr.to_station_id = fs.id


--Ahora empezaremos a limpiar cada una de nuestras tablas, ya que los procesos anteriores fueron solo para unirlas y realizar algunos procesos
SELECT
COUNT(*)
FROM
Cyclistic.dbo.trips_2015
WHERE usertype= 'Dependent'

--Acá borramos los registros 'Dependent' porque no es algo constante en todos los años, es sólo del 2015, solo son 148 registros de más de 300.000 y complica más el análisis
DELETE FROM Cyclistic.dbo.trips_2015
WHERE usertype = 'Dependent'

--Empezamos a limpiar tabla por tabla, acá identificamos los registros con el mismo ID, borramos los duplicados y creamos una tabla con los registros limpios 
--2013
SELECT 
trip_id,COUNT(*) 
FROM 
Cyclistic.dbo.trips_2013
GROUP BY trip_id
HAVING COUNT(*)> 1

WITH Dups AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY trip_id
            ORDER BY trip_id -- o fecha_inicio, created_at, etc.
        ) AS rn
    FROM Cyclistic.dbo.trips_2013
)DELETE FROM Dups
WHERE rn>1;


SELECT
*
INTO Cyclistic.dbo.clean_trips_2013
FROM
Dups

--Creación de una tabla con todos los años de viajes
SELECT 
trip_id,starttime,from_station_id,to_station_id,distance_km,tripduration_seconds,usertype
Cyclistic.dbo.all_trips
FROM
Cyclistic.dbo.clean_trips_2013

UNION ALL

SELECT 
trip_id,starttime,from_station_id,to_station_id,distance_km,tripduration_seconds,usertype
FROM
Cyclistic.dbo.clean_trips_2014


UNION ALL

SELECT 
trip_id,starttime,from_station_id,to_station_id,distance_km,tripduration_seconds,usertype
FROM
Cyclistic.dbo.clean_trips_2015

UNION ALL

SELECT 
trip_id,starttime,from_station_id,to_station_id,distance_km,tripduration_seconds,usertype
FROM
Cyclistic.dbo.clean_trips_2016

UNION ALL

SELECT 
trip_id,start_time,from_station_id,to_station_id,distance_km,tripduration_seconds,usertype
FROM
Cyclistic.dbo.clean_trips_2017

UNION ALL

SELECT 
trip_id,start_time,from_station_id,to_station_id,distance_km,tripduration_seconds,usertype
FROM
Cyclistic.dbo.clean_trips_2018

UNION ALL

SELECT 
trip_id,starttime,from_station_id,to_station_id,distance_km,tripduration_seconds,usertype
FROM
Cyclistic.dbo.clean_trips_2019


--Creamos una CTE con todos los datos y mediciones que necesitamos para nuestro estudio. Antes de usar las funciones AVG en ambas columnas se hizo el respectivo estudio para asegurarse que no sean valores atípicos que sesguen los resultados
WITH cte AS ( 
SELECT 
YEAR(starttime) AS year, usertype, COUNT(*) AS conteo_viajes,  ROUND(AVG(distance_km),2) AS avg_distance_km, ROUND(AVG(tripduration_seconds),2) AS avg_trip_duration_seconds
FROM
Cyclistic.dbo.all_trips
GROUP BY YEAR(starttime), usertype
)SELECT
*
FROM
cte
ORDER BY year ASC

--Ahora creamos una tabla donde se clasifican en corto, mediano y largo cada viaje para después agruparlos y tener una vista completa
WITH cte AS(
SELECT 
YEAR(starttime) AS	year,distance_km,tripduration_seconds,
CASE 
	WHEN tripduration_seconds <= 3600 THEN 'Corto'
	WHEN tripduration_seconds <=10800 THEN 'Mediano'
	ELSE 'Largo'
END AS clasification_trip,
usertype
FROM 
Cyclistic.dbo.all_trips
) SELECT 
year,clasification_trip, COUNT(*)AS count_clasification, usertype
FROM
cte
GROUP BY year,clasification_trip,usertype
ORDER BY year