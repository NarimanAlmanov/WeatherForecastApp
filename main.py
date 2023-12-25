import requests
import sqlite3
import pytz
import os
from datetime import datetime, timedelta

"""This class is responsible for making requests to API of Open Weather Map and returning responses"""
class WeatherApi:
    def __init__(self, api_key, city):
        self.api_key = api_key
        self.city = city

    def querry_weather(self):
        url = "https://api.openweathermap.org/data/2.5/forecast"
        params = {
                "q": self.city,
                "cnt": 40,
                "units": "metric",
                "appid": self.api_key
                }
        response = requests.get(url, params=params)
        return response

"""This class is responsible for user interaction and converting responses from API"""
class WeatherApp:
    def __init__(self, timezone, weather_api, weather_db):
        self.timezone = timezone
        self.weather_api = weather_api
        self.weather_db = weather_db

    def create_weather_info_list_from_api(self, response):
        if response.json()['cod'] != '200':
            print("Error: ", response.json()['message'])
            return []
        weather_info_list = []
        for time_stamp in response.json()['list']:
            weather_info = WeatherInfo() \
                    .set_time_stamp(datetime.utcfromtimestamp(time_stamp['dt'])) \
                    .set_temp(time_stamp['main']['temp']) \
                    .set_feels_like(time_stamp['main']['feels_like']) \
                    .set_humid(time_stamp['main']['humidity']) \
                    .set_pressure(time_stamp['main']['pressure']) \
                    .set_clouds(time_stamp['clouds']['all']) \
                    .set_condition(time_stamp['weather'][0]['description']) \
                    .set_wind_speed(time_stamp['wind']['speed'])

            weather_info_list.append(weather_info)
        return weather_info_list

    def create_weather_info_list_from_db(self, weather_db):
        weather_info_list = []
        for i in range(40):
            forecast_raw = weather_db.read_from_db(i+1)
            weather_info = WeatherInfo() \
                .set_time_stamp(datetime.strptime(forecast_raw[1], "%Y-%m-%d %H:%M:%S")) \
                .set_condition(forecast_raw[2]) \
                .set_temp(forecast_raw[3]) \
                .set_feels_like(forecast_raw[4]) \
                .set_wind_speed(forecast_raw[5]) \
                .set_humid(forecast_raw[6]) \
                .set_pressure(forecast_raw[7]) \
                .set_clouds(forecast_raw[8])
            weather_info_list.append(weather_info)
        return weather_info_list

    def print_weather_info_list(self, weather_info_list):
        for forecast in weather_info_list:
            forecast.print(self.timezone)
            choice = input("Continue? (Y/n): ")
            if choice.lower() == 'n':
                return

    def get_weather(self):
        """Check date of the first forecst. Update DB if the first forecast is outdated"""
        if self.weather_db.empty == True:
            """If database is empty"""
            weather_info_list = self.create_weather_info_list_from_api(self.weather_api.querry_weather())
            """If API returns error"""
            if len(weather_info_list) == 0:
                return
            else:
                self.weather_db.write_to_db(weather_info_list)
                self.print_weather_info_list(weather_info_list)
                return weather_info_list
        else:
            try:
                forecast = self.weather_db.read_from_db(1)
                time_date = datetime.strptime(forecast[1], "%Y-%m-%d %H:%M:%S")
                if datetime.utcnow() > time_date:
                    weather_info_list = self.create_weather_info_list_from_api(self.weather_api.querry_weather())
                    """If API returns error"""
                    if weather_info_list == []:
                        return
                    else:
                        self.weather_db.clear_db()
                        self.weather_db.write_to_db(weather_info_list)
                        self.print_weather_info_list(weather_info_list)
                else:
                    weather_info_list = self.create_weather_info_list_from_db(self.weather_db)
                    self.print_weather_info_list(weather_info_list)
            except:
                """If database was corrupted"""
                self.weather_db.recreate_db()
                self.weather_db.write_to_db(weather_info_list)
                self.print_weather_info_list(weather_info_list)
            return weather_info_list

"""This class is responsible for storing weather forecast for certain timestamp from API response"""
class WeatherInfo:
    def set_condition(self, condition):
        """Set the weather condition."""
        self.condition = condition
        return self

    def set_temp(self, temp):
        """Set the temperature."""
        self.temp = temp
        return self

    def set_feels_like(self, feels_like):
        """Set the 'feels like' temperature."""
        self.feels_like = feels_like
        return self

    def set_humid(self, humid):
        """Set the humidity level."""
        self.humid = humid
        return self

    def set_pressure(self, pressure):
        """Set atmospheric pressure."""
        self.pressure = pressure
        return self

    def set_wind_speed(self, wind_speed):
        """Set wind speed."""
        self.wind_speed = wind_speed
        return self

    def set_clouds(self, clouds):
        """Set cloud cover."""
        self.clouds = clouds
        return self

    def set_time_stamp(self, time_stamp):
        """Set time stamp."""
        self.time_stamp = time_stamp
        return self

    def print(self, timezone):
        try:
            local_timezone = pytz.timezone(timezone)
            utc_time = self.time_stamp
            local_time = utc_time.replace(tzinfo=pytz.utc).astimezone(local_timezone) 
        except:
            print("Error: unrecognized timezone. Using UTC instead")
            local_time = self.time_stamp
        print(f"Time: {local_time}")
        print(f"Condition: {self.condition}")
        print(f"Temperature: {self.temp}°C")
        print(f"Feels like: {self.feels_like}°C")
        print(f"Wind speed: {self.wind_speed} m/s")
        print(f"Humidity: {self.humid}%")
        print(f"Atmospheric pressure: {self.pressure} hPa")
        print(f"Clouds: {self.clouds}%")

"""This class saves weather forecasts in local database and reads forecasts from local database"""
class WeatherDB:
    def __init__(self, db_path="weather_data.db"):
        self.db_path = db_path

        """Create database if it does not exist"""
        if not os.path.exists(self.db_path):
            self._create_database()
        
        """Check if empty"""
        try:
            self.check_if_empty()
        except:
            """Database is corrupted"""
            self.recreate_db()
            self.empty = True
            print("Error: Database has been corrupted. Recreating database")

    def recreate_db(self):
        os.remove(self.db_path)
        self._create_database()

    def check_if_empty(self):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM weather_info_table")
        count = cursor.fetchone()[0]
        conn.close()
        self.empty = count==0
        return self.empty

    def _create_database(self):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        cursor.execute("""
                        CREATE TABLE weather_info_table (
                            id INTEGER PRIMARY KEY AUTOINCREMENT,
                            date_time DATETIME,
                            condition TEXT,
                            temperature REAL,
                            feels_like REAL,
                            wind_speed REAL,
                            humidity REAL,
                            pressure REAL,
                            clouds REAL
                        )
                       """)
        conn.commit()
        conn.close()

    def write_to_db(self, weather_info_list):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        for forecast in weather_info_list:
            cursor.execute( \
                    """INSERT INTO weather_info_table 
                    (
                        date_time,
                        condition,
                        temperature,
                        feels_like,
                        wind_speed,
                        humidity,
                        pressure,
                        clouds
                    ) 
                    VALUES 
                    (?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        forecast.time_stamp,
                        forecast.condition,
                        forecast.temp,
                        forecast.feels_like,
                        forecast.wind_speed,
                        forecast.humid,
                        forecast.pressure,
                        forecast.clouds
                    )
                )
        conn.commit()
        conn.close()

    def read_from_db(self, index):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM weather_info_table WHERE id=?", (index,))
        forecast_raw = cursor.fetchone() 
        conn.close()
        return forecast_raw

    def clear_db(self):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute("DELETE FROM weather_info_table")
        conn.commit()
        cursor.execute("VACUUM")
        conn.commit()
        conn.close()

"""Load config data from file"""
with open('config', 'r') as file:
    lines = file.readlines()
    config = {line.split(':')[0].strip(): line.split(':')[1].strip() for line in lines}

"""Loading program settings"""
api_key = config.get('api_key')
city = config.get('city')
timezone = config.get('timezone')

"""Making objects to work with"""
weather_api = WeatherApi(api_key, city)
weather_db = WeatherDB()
weather_app = WeatherApp(timezone, weather_api, weather_db)

"""Get weather"""
weather_app.get_weather()
