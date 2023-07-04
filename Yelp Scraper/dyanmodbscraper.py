


import json
import boto3
import datetime
#from botocore.vendored import requests
#import urllib3
import requests
#http = urllib3.PoolManager()
#r = http.request('GET', 'http://httpbin.org/robots.txt')

from decimal import *
from time import sleep





client = boto3.resource(service_name='dynamodb')
                     
table = client.Table('yelp-restaurants')
restaurants = {}
def lambda_handler(event, context):
   
    cuisines = ['french','indian', 'thai', 'mediterranean', 'chinese', 'italian','japanese']
    headers = {'Authorization': 'Bearer qLytlQTgiHVA19rZYxLoC3tWVNmVsx8RT6CUKgesbvz48O-ciWbAZXKfPbvOsRpDIjzF7IBhUByhhY2QXiihim5ahos6h48xx6UEkZ1TDcyp7G3QIyQU-VtK0m5GY3Yx'}
    for cuisine in cuisines:
        for i in range(0, 1000, 50):
            params = {'location': 'Manhattan', 'offset': i, 'limit': 50, 'term': cuisine + " restaurants"}
            response = requests.get("https://api.yelp.com/v3/businesses/search", headers = headers, params=params)
            res = response.json()
            addItemstoDB(res["businesses"], cuisine)






def addItemstoDB(records, cuisine):
   global restaurants
   with table.batch_writer() as batch:
        for rest in records:
            try:
                if rest["alias"] in restaurants:
                    continue;
                rest["Business ID"] = str(rest["id"])
                rest["rating"] = Decimal(str(rest["rating"]))
                restaurants[rest["alias"]] = 0
                rest["coordinates"]["latitude"] = Decimal(str(rest["coordinates"]["latitude"]))
                rest["coordinates"]["longitude"] = Decimal(str(rest["coordinates"]["longitude"]))
                rest['address'] = rest['location']['display_address']
                rest['cuisine'] = cuisine
                rest['insertedAtTimestamp'] = str(datetime.datetime.now())
                rest.pop("transactions", None)
                rest.pop("display_phone", None)
                rest.pop("distance", None)
                rest.pop("location", None)
                rest.pop("categories", None)
                if rest["phone"] == "":
                    rest.pop("phone", None)
                if rest["image_url"] == "":
                    rest.pop("image_url", None)

               
                batch.put_item(Item=rec)
                sleep(0.001)
            except Exception as e:
                print(e)
                print(rest)


