

import json
import boto3
import datetime
import requests
from decimal import *
from time import sleep
from requests_aws4auth import AWS4Auth
#from opensearchpy import OpenSearch, RequestsHttpConnection
#from elastic_transport import RequestsHttpConnection
#from opensearchpy import OpenSearch, RequestsHttpConnection
from elasticsearch import Elasticsearch,RequestsHttpConnection#,Urllib3HttpConnection
#import RequestsHttpConnection






region = 'us-east-1'
service = 'es'
credential = boto3.Session(region_name="us-east-1").get_credentials()
auth = AWS4Auth(credential.access_key, credential.secret_key, region, service,session_token=credential.token)




elasticSearchsEndPoint = 'search-restaurants-qq3egiabgr6mjj2q46kkw25v5a.us-east-1.es.amazonaws.com'

es = Elasticsearch(
    hosts = [{'host': elasticSearchsEndPoint, 'port': 443}],
    http_auth = auth,
    use_ssl = True,
    verify_certs = True,
    #connection_class = Urllib3HttpConnection
    connection_class = RequestsHttpConnection
)
es.info()
es.ping()



restaurants = {}
def lambda_handler(event, context):
    cuisines = ['french','indian', 'thai', 'mediterranean', 'chinese', 'italian','japanese']
    headers = {'Authorization': 'Bearer qLytlQTgiHVA19rZYxLoC3tWVNmVsx8RT6CUKgesbvz48O-ciWbAZXKfPbvOsRpDIjzF7IBhUByhhY2QXiihim5ahos6h48xx6UEkZ1TDcyp7G3QIyQU-VtK0m5GY3Yx'}
    for cuisine in cuisines:
        for i in range(0, 1000, 50):
            params = {'location': 'Manhattan', 'offset': i, 'limit': 50, 'term': cuisine + " restaurants"}
            response = requests.get("https://api.yelp.com/v3/businesses/search", headers = headers, params=params)
            resp = response.json()
            addItems(resp["businesses"], cuisine)




def addItems(record, cuisine):
    for rec in record:
            records = {}
            try:
                if rec["alias"] in restaurants:
                    continue;
                records['cuisine'] = cuisine
                records['Business ID'] = str(rec["id"])
                sleep(0.001)
                print(records)
                es.index(index="restaurants", doc_type="Restaurants", body=records)
            except Exception as e:
                print(e)
        











