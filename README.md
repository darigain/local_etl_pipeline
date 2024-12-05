# **ETL Pipeline for Weather, Flights, and City Insights**

## **Overview**
This repository showcases an ETL (Extract, Transform, Load) pipeline designed to collect, process, and integrate data about weather, flights, and city demographics. The project demonstrates how to build a reliable data pipeline for extracting external data, transforming it into usable formats, and loading it into a relational database for analysis and insights.

Why does this matter? By integrating weather forecasts, flight schedules, and city demographics, the pipeline supports practical use cases such as:
- Understanding weather trends to predict demand for outdoor services.
- Analyzing flight schedules to anticipate tourist influx.
- Leveraging city population and location data for urban planning or mobility optimization.

Whether you are building dashboards, developing predictive models, or exploring urban trends, this pipeline provides a solid foundation for data-driven decision-making.

---

## **Data Highlights**
### **Weather Data**:
- Collected real-time forecasts for selected cities using the OpenWeather API.
- Includes temperature, humidity, wind speed, and weather descriptions for actionable insights.

### **Flight Data**:
- Retrieved information on scheduled arrivals for nearby airports via the Aerodatabox API.
- Provides key data points like flight numbers, departure airports, and scheduled arrival times.

### **City Insights**:
- Scraped city population, latitude, and longitude from Wikipedia.
- Data includes city names, countries, and geographic coordinates, enabling location-based analysis.

---

## **Features**
- **Extract**:
  - Utilizes APIs (OpenWeather, Aerodatabox) and web scraping to gather data.
  - Combines structured and semi-structured data sources into a cohesive dataset.
- **Transform**:
  - Cleans and formats raw data, such as converting geographic coordinates from DMS to decimal format.
  - Ensures compatibility across APIs and database systems.
- **Load**:
  - Stores processed data in a relational MySQL database for easy access and analysis.
  - Organizes data into normalized tables for weather, flights, and cities.

---

## **Languages and Tools**
- **Languages**: Python, SQL
- **APIs**: OpenWeather API, Aerodatabox API
- **Libraries**:
  - `requests` and `BeautifulSoup` for data extraction
  - `pandas` for transformation and data handling
  - `sqlalchemy` for database integration
- **Database**: MySQL for relational data storage

---

## **Database Schema**
### **Tables**:
1. **Cities**: Contains city names, countries, and geographic coordinates.
2. **Population**: Tracks city population data with timestamps for historical insights.
3. **Weather**: Includes forecasts like temperature, humidity, and weather descriptions for each city.
4. **Airports**: Lists nearby airports with ICAO codes, names, and city associations.
5. **Flights**: Details flight schedules, including arrival times, departure airports, and flight numbers.
