-- Créer la base de données 'airline' si elle n'existe pas déjà
CREATE DATABASE IF NOT EXISTS airline;

-- Utiliser la base de données 'airline'
USE airline;

-- Créer une table externe pour les données de vol au format Parquet, partitionnée par année
CREATE EXTERNAL TABLE IF NOT EXISTS pq_flight_part 
(
    month TINYINT,
    dayofmonth TINYINT,
    dayofweek TINYINT,
    deptime SMALLINT,
    crsdeptime SMALLINT,
    arrtime SMALLINT,
    crsarrtime SMALLINT,
    uniquecarrier STRING,
    flightnum STRING,
    tailnum STRING,
    actualelapsedtime SMALLINT,
    crselapsedtime SMALLINT,
    airtime SMALLINT,
    arrdelay SMALLINT,
    depdelay SMALLINT,
    origin STRING,
    dest STRING,
    distance SMALLINT,
    taxiin SMALLINT,
    taxiout SMALLINT,
    cancelled SMALLINT,
    cancellationcode STRING,
    diverted SMALLINT,
    carrierdelay SMALLINT,
    weatherdelay SMALLINT,
    nasdelay SMALLINT,
    securitydelay SMALLINT,
    lateaircraftdelay SMALLINT
)
PARTITIONED BY (year SMALLINT)
STORED AS PARQUET
LOCATION '/user/cloudera/output/airline/pq_flight_part';

-- Ajouter des partitions pour les années de 2003 à 2008
ALTER TABLE pq_flight_part ADD PARTITION (year=2003);
ALTER TABLE pq_flight_part ADD PARTITION (year=2004);
ALTER TABLE pq_flight_part ADD PARTITION (year=2005);
ALTER TABLE pq_flight_part ADD PARTITION (year=2006);
ALTER TABLE pq_flight_part ADD PARTITION (year=2007);
ALTER TABLE pq_flight_part ADD PARTITION (year=2008);

-- Définir le nombre de tâches de réduction pour l'insertion
SET mapred.reduce.tasks=1;

-- Insérer les données de 2004 dans la table partitionnée en supprimant les doublons
INSERT OVERWRITE TABLE pq_flight_part PARTITION (year=2004)
SELECT DISTINCT
    month, dayofmonth, dayofweek, deptime, crsdeptime, arrtime, crsarrtime, 
    uniquecarrier, flightnum, tailnum, actualelapsedtime, crselapsedtime, airtime, arrdelay, depdelay, 
    origin, dest, distance, taxiin, taxiout, cancelled, cancellationcode, diverted, carrierdelay, 
    weatherdelay, nasdelay, securitydelay, lateaircraftdelay
FROM pq_flight 
WHERE year = 2004;

-- Insérer les données des autres années (sauf 2003) dans la table partitionnée
INSERT OVERWRITE TABLE pq_flight_part PARTITION (year)
SELECT 
    month, dayofmonth, dayofweek, deptime, crsdeptime, arrtime, crsarrtime, 
    uniquecarrier, flightnum, tailnum, actualelapsedtime, crselapsedtime, airtime, arrdelay, depdelay, 
    origin, dest, distance, taxiin, taxiout, cancelled, cancellationcode, diverted, carrierdelay, 
    weatherdelay, nasdelay, securitydelay, lateaircraftdelay, year
FROM pq_flight 
WHERE year != 2003;

-- Création d'une vue v_flights_denom avec des informations enrichies sur les vols
CREATE VIEW v_flights_denom AS 
SELECT 
    year,
    month,
    CAST(CONCAT(CAST(year AS STRING), CAST(month AS STRING)) AS SMALLINT) AS yearmonth,
    dayofmonth,
    CASE dayofweek 
        WHEN 1 THEN 'Monday'
        WHEN 2 THEN 'Tuesday'
        WHEN 3 THEN 'Wednesday'
        WHEN 4 THEN 'Thursday'
        WHEN 5 THEN 'Friday'
        WHEN 6 THEN 'Saturday'
        WHEN 7 THEN 'Sunday'
        ELSE 'Unknown'
    END AS dayofweek, 
    flightnum, 
    deptime, 
    crsdeptime, 
    arrtime, 
    crsarrtime, 
    actualelapsedtime,
    crselapsedtime, 
    airtime, 
    arrdelay, 
    depdelay, 
    origin, 
    pao.airport AS origin_airport,         -- Renomme la colonne airport de pao en origin_airport
    pao.city AS origin_city,
    pao.state AS origin_state,
    pao.country AS origin_country,
    dest, 
    pad.airport AS dest_airport,           -- Renomme la colonne airport de pad en dest_airport
    pad.city AS dest_city,
    pad.state AS dest_state,
    pad.country AS dest_country,
    distance, 
    taxiin, 
    taxiout, 
    cancelled, 
    cancellationcode, 
    diverted, 
    carrierdelay, 
    weatherdelay, 
    nasdelay, 
    securitydelay, 
    lateaircraftdelay,
    pf.uniquecarrier,                     -- Code du transporteur aérien unique
    pc.description AS carrier,             -- Description du transporteur aérien
    pf.tailnum,                            -- Numéro de queue de l'avion
    ppi.type AS plane_type,                -- Type d'avion
    ppi.manufacturer,                      -- Fabricant de l'avion
    ppi.issue_date,                        -- Date d'émission du certificat de l'avion
    IF ((pf.year - CAST(SUBSTR(issue_date, 7, 4) AS INT) < 0), 0, pf.year - CAST(SUBSTR(issue_date, 7, 4) AS INT)) AS age_of_plane, 
    ppi.model, 
    ppi.status, 
    ppi.aircraft_type, 
    ppi.pyear
FROM 
    pq_flight_part pf
LEFT JOIN 
    pq_carriers pc ON pc.cdde = pf.uniquecarrier
LEFT JOIN 
    pq_plane_info ppi ON ppi.tailnum = pf.tailnum
LEFT JOIN 
    pq_airports pao ON pao.iata = pf.origin
LEFT JOIN 
    pq_airports pad ON pad.iata = pf.dest;


-- Créer une vue pour les vols de l'année 2003
CREATE VIEW v_flight_2003 AS 
SELECT * FROM v_flights_denom WHERE year = 2003;

-- Créer une vue pour les vols des années 2003 à 2005
CREATE VIEW v_flight_2003_2005 AS 
SELECT * FROM v_flights_denom WHERE year IN (2003, 2004, 2005);

-- Calculer la moyenne des retards par mois pour l'année 2006
SELECT year, month, AVG(arrdelay) AS arrdelay, AVG(depdelay) AS depdelay 
FROM pq_flight_part 
WHERE year = 2006
GROUP BY year, month;

-- Calculer la moyenne des retards par mois pour les années 2003 à 2005
SELECT year, month, AVG(arrdelay) AS arrdelay, AVG(depdelay) AS depdelay 
FROM pq_flight_part 
WHERE year IN (2003, 2004, 2005)
GROUP BY year, month;

-- Collecte les statistiques sur la table pq_flight_part
COMPUTE STATS pq_flight_part;

-- Cette commande affiche des informations détaillées sur la table pq_flight_part
DESCRIBE FORMATTED pq_flight_part;


-- Calculer la moyenne des retards par mois pour les années 2003 à 2005 (intervalle)
SELECT year, month, AVG(arrdelay) AS arrdelay, AVG(depdelay) AS depdelay 
FROM pq_flight_part 
WHERE year BETWEEN 2003 AND 2005
GROUP BY year, month;
