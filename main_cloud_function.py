import functions_framework
import requests
import pandas as pd
from bs4 import BeautifulSoup
import sqlalchemy
from sqlalchemy.exc import IntegrityError
from sqlalchemy import create_engine, text
import pymysql
from datetime import datetime, timedelta
from pytz import timezone
# Passwords and API keys
import Keys

@functions_framework.http
def extracting_and_loading_data(request):
    # list_of_cities = ["Berlin", "Hamburg", "Munich"]
    connection_string = cloud_connection_string()
    # cities_df = get_cities_data(list_of_cities)
    # cities_to_db = transform_cities_df(cities_df)
    # push_unique_data_to_db(cities_to_db, 'cities', connection_string)
    cities_from_sql = extract_from_db('cities', connection_string)
    # population_to_db = transform_population(cities_df, cities_from_sql)
    # push_unique_data_to_db(population_to_db, 'population', connection_string)
    weather_df = get_weather_data(cities_from_sql)
    # push_data_to_db(weather_df, 'weather', connection_string)
    update_and_insert_data_to_db(weather_df, 'weather', connection_string, ['city_id', 'forecast_time'])
    # airports_df = get_airports_data(cities_from_sql)
    # airports_to_db = transform_airports_df(airports_df)
    # push_unique_data_to_db(airports_to_db, 'airports', connection_string)
    airports_from_sql = extract_from_db('airports', connection_string)
    flights_df = get_flights_data(airports_from_sql)
    # push_data_to_db(flights_df, 'flights', connection_string)
    update_and_insert_data_to_db(flights_df, 'flights', connection_string, ['arrival_airport_icao', 'scheduled_arrival_time', 'flight_number'])
    return "Data has been updated"

def get_cities_data(cities):
  cities_data = []

  for city in cities:
    city_data = {}

    # city
    city_data["City"] = city

    # create the soup
    url = f"https://www.wikipedia.org/wiki/{city}"
    response = requests.get(url)
    city_soup = BeautifulSoup(response.content, 'html.parser')

    # country
    city_data["Country"] = city_soup.find(class_="infobox-data").get_text()

    # population
    city_population = city_soup.find(string="Population").find_next("td").get_text()
    try:
      city_population_clean = int(city_population.replace(",", ""))
    except (ValueError):
      city_population_clean = int('0')
    city_data["Population"] = city_population_clean

    # data retrieved
    city_data["Year_Data_Retrieved"] = city_soup.find(string="Population").find_next().get_text()[2:6]

    # latitude and longitude
    city_data["Latitude"] = city_soup.find(class_="latitude").get_text()
    city_data["Longitude"] = city_soup.find(class_="longitude").get_text()

    # append this city's data to the cities list
    cities_data.append(city_data)

  return pd.DataFrame(cities_data)

def create_connection_string():
  schema = "gans_local"
  host = "localhost" # "127.0.0.1"
  user = "root"
  password = Keys.MySQL_pass
  port = 3306
  return f'mysql+pymysql://{user}:{password}@{host}:{port}/{schema}'

def cloud_connection_string():
  schema = "gans_cloud"
  host = "XX.XXX.XXX.XXX"
  user = "root"
  password = Keys.MySQL_pass
  port = 3306
  return f'mysql+pymysql://{user}:{password}@{host}:{port}/{schema}'

def dms_to_decimal(dms):
    import re
    
    # Regex to extract degrees, minutes, seconds, and hemisphere
    pattern = r"(\d+)°(\d+)?′?(\d+)?″?([NSEW])"
    match = re.match(pattern, dms.strip())
    
    if not match:
        raise ValueError(f"Invalid DMS format: {dms}")
    
    # Extract parts
    degrees = float(match.group(1))
    minutes = float(match.group(2)) if match.group(2) else 0.0
    seconds = float(match.group(3)) if match.group(3) else 0.0
    hemisphere = match.group(4).upper()
    
    # Convert to decimal degrees
    decimal_degrees = degrees + (minutes / 60) + (seconds / 3600)
    
    # Adjust for southern/western hemispheres
    if hemisphere in ('S', 'W'):
        decimal_degrees = -decimal_degrees
    
    return decimal_degrees

def transform_cities_df(cities_df):
  cities_to_db = cities_df[["City", "Country", "Latitude", "Longitude"]].rename(columns={"City": "City_name"})
  cities_to_db["Latitude"] = cities_to_db["Latitude"].apply(dms_to_decimal)
  cities_to_db["Longitude"] = cities_to_db["Longitude"].apply(dms_to_decimal)
  return cities_to_db

def push_unique_data_to_db(dataframe, table_name, connection_string):
    engine = create_engine(connection_string)
    # Get column names from the DataFrame
    columns = list(dataframe.columns)
    column_placeholders = ", ".join(["%s"] * len(columns))
    column_names = ", ".join(columns)

    # Prepare the SQL query
    sql_query = f"""
        INSERT IGNORE INTO {table_name} ({column_names})
        VALUES ({column_placeholders});
    """

    # Execute the query for each row
    with engine.connect() as conn:
        for _, row in dataframe.iterrows():
            conn.execute(sql_query, tuple(row.values))
    # print(f"Data inserted into {table_name} successfully (if not already present).")

def extract_from_db(table_name, connection_string):
  df = pd.read_sql(table_name, con=connection_string)
  return df

def transform_population(cities_df, cities_from_sql):
  population_df = cities_df[["Population", "Year_Data_Retrieved", "City"]].rename(columns={"City": "City_name"})
  # Merge with cities_from_sql to get the correct City_id
  population_to_db = population_df.merge(
      cities_from_sql,
      on=["City_name"],
      how="left",
      suffixes=("", "_db")
  )
  # Keep only the necessary columns for the population table
  population_to_db = population_to_db[["Population", "Year_Data_Retrieved", "City_id"]]
  return population_to_db

def get_weather_data(cities_df):
  berlin_timezone = timezone('Europe/Berlin')
  API_key = Keys.OpenWeather_API_key
  cities_weather_data = []

  for city_index in cities_df.index:
    city_id = cities_df.loc[city_index,'City_id']
    lat = cities_df.loc[city_index,'Latitude']
    lon = cities_df.loc[city_index,'Longitude']

    # Reference the parameters in the url.
    url = (f"https://api.openweathermap.org/data/2.5/forecast?lat={lat}&lon={lon}&appid={API_key}&units=metric")
    response = requests.get(url)
    weather_json = response.json()

    retrieval_time = datetime.now(berlin_timezone).strftime("%Y-%m-%d %H:%M:%S")

    for item in weather_json['list']:
      city_weather_data = {
        'city_id': city_id,
        'forecast_time': item.get("dt_txt"),
        'temperature': item["main"].get("temp"),
        'feels_like': item["main"].get("feels_like"),
        'humidity': item["main"].get("humidity"),
        'forecast_desc': item["weather"][0].get("description"),
        'forecast': item["weather"][0].get("main"),
        'wind_speed': item["wind"].get("speed"),
        'rain_prob': item.get("rain", {}).get("3h", 0),
        'station': weather_json["city"].get("name"),
        'country_code': weather_json["city"].get("country"),
        'data_retrieved_at': retrieval_time
      }
      cities_weather_data.append(city_weather_data)

  weather_df = pd.DataFrame(cities_weather_data)
  weather_df["forecast_time"] = pd.to_datetime(weather_df["forecast_time"])
  weather_df["data_retrieved_at"] = pd.to_datetime(weather_df["data_retrieved_at"])

  return weather_df

def push_data_to_db(df, table_name, connection_string):
  df.to_sql(table_name,
            if_exists='append',
            con=connection_string,
            index=False)

def get_airports_data(df):
    list_for_df = []

    # Loop through the rows of the input DataFrame
    for _, row in df.iterrows():
        url = "https://aerodatabox.p.rapidapi.com/airports/search/location"
        querystring = {
            "lat": row['Latitude'],
            "lon": row['Longitude'],
            "radiusKm": "50",
            "limit": "5",
            "withFlightInfoOnly": "true"
        }
        headers = {
            "X-RapidAPI-Host": "aerodatabox.p.rapidapi.com",
            "X-RapidAPI-Key": Keys.Rapid_API_key
        }

        # Make the API request
        response = requests.request("GET", url, headers=headers, params=querystring)
        airports_data = pd.json_normalize(response.json().get('items', []))
        airports_data['City_id'] = row['City_id']  # Add City_id for merging later
        list_for_df.append(airports_data)
    api_data = pd.concat(list_for_df, ignore_index=True)
    # Merge the original DataFrame with the API results
    merged_df = df.merge(api_data, on='City_id', how='left')
    return merged_df

def transform_airports_df(airports_df):
  # Selecting only the columns we need
  airports_to_db = airports_df[["icao", "iata", "name", "City_id"]]
  airports_to_db = airports_to_db.rename(columns={"name": "Airport_name"})
  return airports_to_db

def get_flights_data(airports_df):
    api_key = Keys.Rapid_API_key

    berlin_timezone = timezone('Europe/Berlin')
    today = datetime.now(berlin_timezone).date()
    tomorrow = (today + timedelta(days=1))

    flight_items = []

    for icao in airports_df["icao"]:
        # The API can only make 12-hour calls
        times = [["00:00", "11:59"], ["12:00", "23:59"]]

        for time in times:
            url = f"https://aerodatabox.p.rapidapi.com/flights/airports/icao/{icao}/{tomorrow}T{time[0]}/{tomorrow}T{time[1]}"

            querystring = {
                "withLeg": "true",
                "direction": "Arrival",
                "withCancelled": "false",
                "withCodeshared": "true",
                "withCargo": "false",
                "withPrivate": "false"
            }

            headers = {
                'x-rapidapi-host': "aerodatabox.p.rapidapi.com",
                'x-rapidapi-key': api_key
            }

            try:
                response = requests.get(url, headers=headers, params=querystring)

                # Validate the response
                if response.status_code == 200 and response.content.strip():
                    try:
                        flights_json = response.json()
                    except ValueError as e:
                        print(f"Error decoding JSON for ICAO={icao}: {e}")
                        print(f"Response Content: {response.content.decode('utf-8', errors='ignore')}")
                        continue
                else:
                    print(f"API returned invalid response for ICAO={icao}: {response.status_code}")
                    print(f"Response Content: {response.content.decode('utf-8', errors='ignore')}")
                    continue

                retrieval_time = datetime.now(berlin_timezone).strftime("%Y-%m-%d %H:%M:%S")
                icao_id = airports_df.loc[airports_df["icao"] == icao, "icao"].values[0]

                for item in flights_json.get("arrivals", []):
                    flight_item = {
                        "arrival_airport_icao": icao_id,
                        "departure_airport_icao": item["departure"]["airport"].get("icao", None),
                        "departure_airport_name": item["departure"]["airport"].get("name", None),
                        "scheduled_arrival_time": item["arrival"]["scheduledTime"].get("local", None),
                        "revised_arrival_time": item["arrival"].get("revisedTime", {}).get("local", None),
                        "flight_number": item.get("number", None),
                        "data_retrieved_at": retrieval_time
                    }
                    flight_items.append(flight_item)

            except requests.exceptions.RequestException as req_err:
                print(f"Request error for ICAO={icao}: {req_err}")

    flights_df = pd.DataFrame(flight_items)
    flights_df["scheduled_arrival_time"] = flights_df["scheduled_arrival_time"].str[:-6]
    flights_df["scheduled_arrival_time"] = pd.to_datetime(flights_df["scheduled_arrival_time"])
    flights_df["revised_arrival_time"] = flights_df["revised_arrival_time"].str[:-6]
    flights_df["revised_arrival_time"] = pd.to_datetime(flights_df["revised_arrival_time"])
    flights_df["revised_arrival_time"] = flights_df["revised_arrival_time"].apply(lambda x: None if pd.isna(x) else x)
    flights_df["revised_arrival_time"] = flights_df["revised_arrival_time"].replace({pd.NaT: None})
    flights_df["data_retrieved_at"] = pd.to_datetime(flights_df["data_retrieved_at"])
    
    return flights_df

def update_and_insert_data_to_db(dataframe, table_name, connection_string, unique_keys):
    engine = create_engine(connection_string)
    columns = list(dataframe.columns)
    column_placeholders = ", ".join([f":{col}" for col in columns])  # Named placeholders for SQLAlchemy
    column_names = ", ".join(columns)

    # Prepare the ON DUPLICATE KEY UPDATE clause
    update_clause = ", ".join([f"{col} = VALUES({col})" for col in columns if col not in unique_keys])

    # update_clause = ", ".join(
    # [f"{col} = VALUES({col})" if col != "data_retrieved_at" else f"{col} = NOW()" for col in columns if col not in unique_keys]
    # )

    # Prepare the SQL query
    sql_query = text(f"""
        INSERT INTO {table_name} ({column_names})
        VALUES ({column_placeholders})
        ON DUPLICATE KEY UPDATE {update_clause};
    """)

    # Execute the query for each row
    with engine.connect() as conn:
        for _, row in dataframe.iterrows():
            conn.execute(sql_query, row.to_dict())
            conn.commit()
    # print(f"Data inserted or updated into '{table_name}' successfully.")