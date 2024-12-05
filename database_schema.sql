-- Drop the database if it already exists
-- DROP DATABASE IF EXISTS gans_local;
-- DROP TABLE IF EXISTS cities;
-- DROP TABLE IF EXISTS population;
-- DROP TABLE IF EXISTS weather;
-- DROP TABLE IF EXISTS airports;
-- DROP TABLE IF EXISTS flights;

-- Create the database
CREATE DATABASE gans_local;

-- Use the database
USE gans_local;


CREATE TABLE cities (
    City_id INT AUTO_INCREMENT, -- Automatically generated ID for each city
    City_name VARCHAR(100) NOT NULL, -- Name of the city
    Country VARCHAR(100) NOT NULL, -- Name of the country
    Latitude FLOAT NOT NULL, -- Latitude
    Longitude FLOAT NOT NULL, -- Longitude
    PRIMARY KEY (City_id), -- Primary key to uniquely identify each city
    UNIQUE (City_name, Country, Latitude, Longitude) -- Unique constraint
);


CREATE TABLE population (
    Population_id INT AUTO_INCREMENT,
    Population INT NOT NULL,
    Year_Data_Retrieved VARCHAR(100),
    City_id INT,
    PRIMARY KEY (Population_id),
    UNIQUE (Population, Year_Data_Retrieved, City_id),
    FOREIGN KEY (City_id) REFERENCES cities(City_id)
);


CREATE TABLE weather (
	weather_id INT AUTO_INCREMENT,
    city_id INT NOT NULL, 
    forecast_time DATETIME,
    temperature FLOAT,
    feels_like FLOAT,
    humidity INT,
    forecast_desc VARCHAR(150),
    forecast VARCHAR(150),
    wind_speed FLOAT,
    rain_prob FLOAT,
    station VARCHAR(150),
    country_code VARCHAR(50),
    data_retrieved_at DATETIME,
    PRIMARY KEY (weather_id),
    FOREIGN KEY (city_id) REFERENCES cities(city_id)
);


CREATE TABLE airports(
    icao VARCHAR(10),
    iata VARCHAR(10),
    Airport_name VARCHAR(255),
    City_id INT NOT NULL,
    PRIMARY KEY (icao),
    FOREIGN KEY (City_id) REFERENCES cities(City_id)
);




CREATE TABLE flights(
	flight_id INT AUTO_INCREMENT,
    arrival_airport_icao VARCHAR(10),
    departure_airport_icao VARCHAR(10),
    departure_airport_name VARCHAR(100),
    scheduled_arrival_time DATETIME,
    flight_number VARCHAR(30),
    data_retrieved_at DATETIME,
    PRIMARY KEY (flight_id),
    FOREIGN KEY (arrival_airport_icao) REFERENCES airports(icao)
);


SELECT * FROM cities;
SELECT * FROM population;
SELECT count(*) FROM weather;
SELECT * FROM airports;
SELECT count(*) FROM flights;