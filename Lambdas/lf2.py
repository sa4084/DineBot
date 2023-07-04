import boto3
import json
import requests
import random
from requests_aws4auth import AWS4Auth

def receiveMsgFromSqsQueue():
    sqs = boto3.client('sqs')
    queue_url = 'https://sqs.us-east-1.amazonaws.com/009309164804/SQSV1'
    response = sqs.receive_message(
        QueueUrl=queue_url,
        AttributeNames=['SentTimestamp'],
        MaxNumberOfMessages=5,
        MessageAttributeNames=['All'],
        VisibilityTimeout=10,
        WaitTimeSeconds=0
        )
    return response


def getRestaurantFromES(cuisine):
    region = 'us-east-1'
    service = 'es'
    credentials = boto3.Session(region_name="us-east-1").get_credentials()
    awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, region, service, session_token=credentials.token)
    host = 'search-restaurants-qq3egiabgr6mjj2q46kkw25v5a.us-east-1.es.amazonaws.com'
    index = 'restaurants'
    url = 'https://' + host + '/' + index + '/_search'
    query = {
        "size": 1300,
        "query": {
            "query_string": {
                "default_field": "cuisine",
                "query": cuisine
            }
        }
    }
    headers = { "Content-Type": "application/json" }
    response = requests.get(url,auth=awsauth, headers=headers, data=json.dumps(query))
    rest_resp = response.json()
    print(rest_resp)

    hits = rest_resp['hits']['hits']
    restaurantIds = []
    for hit in hits:
        restaurantIds.append(str(hit['_source']['Business ID']))
    return restaurantIds

def getRestaurantFromDynamoDb(restaurantIds):
    restaurant = []
    client = boto3.resource('dynamodb')
    table = client.Table('yelp-restaurants')
    for restaurantId in restaurantIds:
        response = table.get_item(Key={'Business ID': restaurantId})
        restaurant.append(response)
    return restaurant

def getEmailToSend(details,message):
    cuisine = message['MessageAttributes']['Cuisine']['StringValue']
    date = message['MessageAttributes']['Date']['StringValue']
    time = message['MessageAttributes']['Time']['StringValue']
    noOfPeople = message['MessageAttributes']['NoOfPeople']['StringValue']
    separator = ', '
    restaurantOneName = details[0]['Item']['name']
    restaurantOneAdd = separator.join(details[0]['Item']['address'])
    restaurantTwoName = details[1]['Item']['name']
    restaurantTwoAdd = separator.join(details[1]['Item']['address'])
    restaurantThreeName = details[2]['Item']['name']
    restaurantThreeAdd = separator.join(details[2]['Item']['address'])
    emailBody = 'Hi! These are some {0} restaurant suggestions for {1} people, for {2} at {3} : 1. {4}, located at {5}, 2. {6}, located at {7},3. {8}, located at {9}.'.format(cuisine,noOfPeople,date,time,restaurantOneName,restaurantOneAdd,restaurantTwoName,restaurantTwoAdd,restaurantThreeName,restaurantThreeAdd)
    return emailBody
    
def sendEmail(emailBody,emailId):
    client = boto3.client("ses")
    
    client.send_email(
            Destination={
                'ToAddresses': [
                    emailId,
                ],
            },
            Message={
                'Body': {
                    
                    'Text': {
        
                        'Data': emailBody
                    },
                },
                'Subject': {

                    'Data': "Restaurants For You"
                },
            },
            Source="aj3087@columbia.edu"
        )
    
def deleteEmail(receipt_handle):
    sqs = boto3.client('sqs')
    queue_url = 'https://sqs.us-east-1.amazonaws.com/009309164804/SQSV1'
    sqs.delete_message(QueueUrl=queue_url,
    ReceiptHandle=receipt_handle
    )

def lambda_handler(event, context):
    print(event)

    sqsResponse = receiveMsgFromSqsQueue()
    if "Messages" in sqsResponse.keys():
        for message in sqsResponse['Messages']:
            cuisine = message['MessageAttributes']['Cuisine']['StringValue']
            restaurantIds = getRestaurantFromES(cuisine)
            
            restaurantIds = random.sample(restaurantIds, 3)
            details = getRestaurantFromDynamoDb(restaurantIds)
            
            emailBody = getEmailToSend(details,message)
            print(emailBody)
            
            emailId = message['MessageAttributes']['PhoneNumber']['StringValue']
           
            sendEmail(emailBody,emailId)
            receipt_handle = message['ReceiptHandle']
            deleteEmail(receipt_handle)