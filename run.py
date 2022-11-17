import os
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
import requests
import json
import dateutil.parser
import time
from dotenv import load_dotenv
import geocoder
from datetime import datetime
import calendar

load_dotenv()

username = os.getenv("MONGO_USERNAME")
password = os.getenv("MONGO_PASSWORD")

client = MongoClient(
    f"mongodb+srv://{username}:{password}@self-service-bicycle.kg5bfrx.mongodb.net/?retryWrites=true&w=majority", server_api=ServerApi('1'))

db = client.vls


def get_vlille():
    url = os.getenv("VLILLE_URL")
    response = requests.request("GET", url)
    response_json = json.loads(response.text.encode('utf8'))
    return response_json.get("records", [])


vlilles = get_vlille()

vlilles_to_insert = [
    {
        '_id': elem.get('fields', {}).get('libelle'),
        'name': elem.get('fields', {}).get('nom', '').title(),
        'geometry': elem.get('geometry'),
        'size': elem.get('fields', {}).get('nbvelosdispo') + elem.get('fields', {}).get('nbplacesdispo'),
        'source': {
            'dataset': elem.get('fields', {}).get('commune'),
            'id_ext': elem.get('fields', {}).get('libelle')
        },
        'tpe': elem.get('fields', {}).get('type', '') == 'AVEC TPE'
    }
    for elem in vlilles
]

try:
    db.stations.insert_many(vlilles_to_insert, ordered=False)
except:
    pass

db.stations.create_index([("geometry", "2dsphere")])

# (3) Client program:


def get_nearest_station(lat, lng):
    return list(db.stations.find(
        {
            "geometry": {
                "$nearSphere": {
                    "$geometry": {
                        "type": "Point",
                        "coordinates": [lng, lat]
                    }
                }
            }
        }
    ).limit(3))

# (4) Business program - for the city

# find station with name (with some letters)


def get_station_by_name(stationName):
    return list(db.stations.find({'name': {'$regex': stationName, '$options': 'i'}}))


#  delete a station
def delete_station(stationName):
    db.stations.delete_one({'name': stationName})


#  update a station
def update_station(stationName, geometry=None, size=None, source=None, tpe=None, name=None, active=None):
    if geometry:
        db.stations.update_one({'name': stationName}, {
                               '$set': {'geometry': geometry}})
    if size:
        db.stations.update_one({'name': stationName}, {'$set': {'size': size}})
    if source:
        db.stations.update_one({'name': stationName}, {
                               '$set': {'source': source}})
    if tpe:
        db.stations.update_one({'name': stationName}, {'$set': {'tpe': tpe}})
    if name:
        db.stations.update_one({'name': stationName}, {'$set': {'name': name}})
    if active:
        db.stations.update_one({'name': stationName}, {
                               '$set': {'active': active}})


def distance(point1, point2):
    lat1, lng1 = point1
    lat2, lng2 = point2
    g = geocoder.google([lat1, lng1], method='reverse')
    g2 = geocoder.google([lat2, lng2], method='reverse')
    return g.distance(g2, units='m')

#  desactivate all stations in an area


def desactivate_stations_in_area(point):
    # get distance between point and all stations
    stations = list(db.stations.find())
    for station in stations:
        # if distance > 1000m => desactivate
        print(station["name"], distance(point, station["geometry"]
              ["coordinates"]), 'm')
        if distance(point, station["geometry"]["coordinates"]) > 1000:
            db.stations.update_one({'name': station["name"]}, {
                                   '$set': {'active': False}})
    return list(db.stations.find({'active': False}))


# desactivate_stations_in_area([3.0667, 50.6333])

# - give all stations with a ratio bike/total_stand under 20%
def get_station_by_ratio():
    stations = list(db.stations.find())
    for station in stations:
        # get data of the station
        data = db.data.find_one({'station_id': station["_id"]})
        if data:
            # get ratio
            ratio = data["bike_stands"] / data["available_bike_stands"]
            if ratio < 0.2:
                print(station["name"], ratio)
    return list(db.stations.find({'active': False}))


delete_station("Bailly")
# update_station("Palais Rameau", "name", "Palais-Rameau")

stationName = input("Entrez une station : ")
print(get_station_by_name(stationName))

get_station_by_ratio()

while True:
    print('\n************ UPDATE ************ \n')
    vlilles = get_vlille()
    datas = [
        {
            "bike_availbale": elem.get('fields', {}).get('nbvelosdispo'),
            "stand_availbale": elem.get('fields', {}).get('nbplacesdispo'),
            "date": dateutil.parser.parse(elem.get('fields', {}).get('datemiseajour')),
            "station_id": elem.get('fields', {}).get('libelle')
        }
        for elem in vlilles
    ]

    for data in datas:
        db.datas.update_one({'date': data["date"], "station_id": data["station_id"]}, {
                            "$set": data}, upsert=True)

    g = geocoder.ip('me')
    [lat, lng] = g.latlng
    # ISEN 50.633992, 3.048755
    for station in get_nearest_station(50.633992, 3.048755):
        # print bike_availbale and stand_availbale for the 3 nearest stations to your location
        data = db.datas.find_one(
            {"station_id": station["_id"]}, sort=[("date", -1)])
        print(station["name"], ':\n    - ', data["bike_availbale"], ' vélos disponibles, ', data["stand_availbale"], '\n    - places disponibles\n    - distance : ',
              round(geocoder.distance([50.633992, 3.048755], [station["geometry"]["coordinates"][1], station["geometry"]["coordinates"][0]]) * 1000), 'm')
        # print lenght between your location and the station with latitude and longitude

    # print(get_station_by_ratio(datas))

    time.sleep(10)
