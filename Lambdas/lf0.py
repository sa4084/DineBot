import json
import boto3

# botName='DiningConcierge',
# botAlias='DiningConciergeAliasVersionOne',
# userId='009309164804'

def lambda_handler(event, context):
    print(event)
    userIdVal = '009309164804'
    client = boto3.client('lex-runtime')
    response = client.post_text(
    botName='DiningConcierge',
    botAlias='DiningConciergeAliasVersionOne',
    userId=userIdVal,
    inputText=event["messages"][0]["unstructured"]["text"]
    )
    
    if response['ResponseMetadata']['HTTPStatusCode'] == 200 :
        return {
        'statusCode': 200,
        'headers': {
            'Access-Control-Allow-Origin': '*'
        },
        "messages": [
            {
            "type": "unstructured",
            "unstructured": {
                "id": "string",
                "text": response['message'],
                "timestamp": "string"
                }
            }
        ]
        }
    else :
        return {
        'statusCode': 200,
        'headers': {
            'Access-Control-Allow-Origin': '*'
        },
        "messages": [
            {
            "type": "unstructured",
            "unstructured": {
                "id": "string",
                "text": 'Failed at LF0',
                "timestamp": "string"
                }
            }
        ]
        }



