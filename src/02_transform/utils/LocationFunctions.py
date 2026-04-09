import os
import requests
import re
from google.cloud import bigquery
from dotenv import load_dotenv
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, DoubleType, StringType

load_dotenv()
KEY = os.getenv('AZURE_KEY')
CLIENT_ID = os.getenv('CLIENT_ID')
API_VERSION = os.getenv('API_VERSION')

LocationDetailSchema = StructType([
    StructField('City', StringType(), True),
    StructField('Location', StringType(), True),
    StructField('Latitude', DoubleType(), True),
    StructField('Longitude', DoubleType(), True),
    StructField('High_Accuracy', DoubleType(), True)
])

accuracy_map = {
    "High": 1,
    "Medium": 0.5,
    "Low": 0
}

def load_locations_df(spark, table_id):
    return spark.read.format("bigquery") \
                .option('table', table_id) \
                .load()

def get_locations_from_bq(df_locations, raw_locations):
    return df_locations.join(raw_locations, on="Location", how='right')

def get_missing_locations(enriched_df):
    # Get null values in City
    missing_df = enriched_df.filter(F.col('City').isNull())

    # Get unique values 
    missing_locations_df = missing_df.select("location").distinct()

    # Convert to array
    missing_locations = [row.location for row in missing_locations_df.collect()]

    return missing_locations

def update_locations_bq(new_locations_df, staging_locations_table_id, project_id, dataset):
    # upload new locations to staging table in bq
    new_locations_df.write \
            .format("bigquery") \
            .option('temporaryGcsBucket', 'tempresolvedlocation') \
            .option('table', staging_locations_table_id) \
            .mode("append") \
            .save()
    
    # Call the stored procedure to upsert data from staging to final table
    client = bigquery.Client()
    query = f"CALL `{project_id}.{dataset}.upsert_locations`()"
    client.query(query).result() 


def get_geocode(location):
    url = "https://atlas.microsoft.com/geocode"

    params = {
        "api-version": "2026-01-01",
        "addressLine": location,
        "adminDistrict": "Metro Manila",
        "countryRegion": "PH",
        "top": 1
    }

    headers = {
        "Accept-Language": "en-US",
        "x-ms-client-id": CLIENT_ID,
        "subscription-key": KEY
    }

    response = requests.get(url=url, params=params, headers=headers)
    data = response.json()

    longitude = data['features'][0]['geometry']['coordinates'][0]
    latitude = data['features'][0]['geometry']['coordinates'][1]
    city = data['features'][0]['properties']['address']['locality']
    accuracy = accuracy_map.get(data['features'][0]['properties']['confidence'], 0)

    return (latitude, longitude, city.upper(), accuracy)

def get_batch_geocode(spark, locations):
    batch_size = 95  # maximum items per batch
    all_details = []

    # Split locations into batches of 95
    for i in range(0, len(locations), batch_size):
        batch_locations = locations[i:i + batch_size]
        body = create_batch_items(batch_locations)

        url = 'https://atlas.microsoft.com/geocode:batch'
        params = {"api-version": "2026-01-01"}
        headers = {
            "Accept-Language": "en-US",
            "x-ms-client-id": CLIENT_ID,
            "subscription-key": KEY
        }

        response = requests.post(url=url, params=params, headers=headers, json=body)
        data = response.json()
        items = data.get('batchItems', [])

        for idx, item in enumerate(items):
            feature = item.get('features', [None])[0] if item.get('features') else None

            if feature:
                geometry = feature.get('geometry', None)
                coordinates = geometry.get('coordinates', [0.0, 0.0]) if geometry else [0.0, 0.0]
                longitude, latitude = float(coordinates[0]), float(coordinates[1])

                properties = feature.get('properties', None)
                address = properties.get('address', {}) if properties else {}
                city = address.get('locality') or get_reverse_geocode(longitude, latitude)

                confidence = properties.get('confidence', None) if properties else None
                accuracy = float(accuracy_map.get(confidence, 0))
            else:
                city = ''
                longitude = latitude = 0.0
                accuracy = 0.0

            # Normalize city names
            if city in ["Pasay", "Pasig", "Makati"]:
                city = city.strip() + " City"
            if city == "Kalookan City":
                city = "Caloocan City"
            if re.fullmatch(r'Para.*aque', city):
                city = "Paranaque"

            all_details.append({
                "Location": batch_locations[idx],
                "City": city.upper(),
                "Latitude": latitude,
                "Longitude": longitude,
                "High_Accuracy": accuracy
            })

    # Flatten data for Spark DataFrame
    flattened_data = [
        (d["City"], d["Location"], d["Latitude"], d["Longitude"], d["High_Accuracy"])
        for d in all_details
    ]

    df = spark.createDataFrame(flattened_data, LocationDetailSchema)
    return df


def create_batch_items(locations):

    batch_items = []

    for loc in locations:
        batch_items.append({
            "addressLine": loc,
            "adminDistrict": "Metro Manila",
            "countryRegion": "PH",
            "top": 1
        })

    return {"batchItems": batch_items}


def get_reverse_geocode(longitude, latitude):

    coordinates = str(longitude) + "," + str(latitude)
    url = 'https://atlas.microsoft.com/reverseGeocode'

    params = {
        "api-version": "2026-01-01",
        "coordinates": coordinates
    }

    headers = {
        "Accept-Language": "en-US",
        "x-ms-client-id": CLIENT_ID,
        "subscription-key": KEY
    }

    response = requests.get(url=url, params=params, headers=headers)
    data = response.json()
    city = data['features'][0]['properties']['address']['locality']

    return city.upper()