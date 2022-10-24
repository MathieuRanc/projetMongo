import os
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
import requests
import json
import dateutil.parser
import time
from dotenv import load_dotenv
import geocoder

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


while True:
    print('update')
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
        print(station["name"], ': ', data["bike_availbale"],
              ' vélos disponibles, ', data["stand_availbale"], ' places disponibles')

    time.sleep(10)
