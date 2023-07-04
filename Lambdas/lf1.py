import os
import dateutil.parser
import logging
import boto3
import json
import datetime
import time


def sendMsg(slots):
    sqs = boto3.client('sqs')
    values={
        'NoOfPeople': {
            'DataType': 'String',
            'StringValue': slots["NumberOfPeople"]
        },
        'Date': {
            'DataType': 'String',
            'StringValue': slots["DiningDate"]
        },
        'Time': {
            'DataType': 'String',
            'StringValue': slots["DiningTime"]
        },
        'PhoneNumber' : {
            'DataType': 'String',
            'StringValue': slots["PhoneNumber"]
        },
        'Cuisine': {
            'DataType': 'String',
            'StringValue': slots["Cuisine"]
        }
    }
    response = sqs.send_message(
        QueueUrl='https://sqs.us-east-1.amazonaws.com/009309164804/SQSV1',
        MessageAttributes=values,
        MessageBody=('Testing queue')
        )

def elicit_slot(session_attributes, intent_name, slots, slot_to_elicit, message):
    return {
        'sessionAttributes': session_attributes,
        'dialogAction': {
            'type': 'ElicitSlot',
            'intentName': intent_name,
            'slots': slots,
            'slotToElicit': slot_to_elicit,
            'message': message
        }
    }

def close(session_attributes, fulfillment_state, message):
    response = {
        'sessionAttributes': session_attributes,
        'dialogAction': {
            'type': 'Close',
            'fulfillmentState': fulfillment_state,
            'message': message
        }
    }

    return response

def delegate(session_attributes, slots):
    return {
        'sessionAttributes': session_attributes,
        'dialogAction': {
            'type': 'Delegate',
            'slots': slots
        }
    }


def confirm_intent(session_attributes, intent_name, slots, message):
    return {
        'sessionAttributes': session_attributes,
        'dialogAction': {
            'type': 'ConfirmIntent',
            'intentName': intent_name,
            'slots': slots,
            'message': message
        }
    }


def validateall(isvalid, violated_slot, message_content):
    return {
        'isValid': isvalid,
        'violatedSlot': violated_slot,
        'message': {'contentType': 'PlainText', 'content': message_content}
    }


def validatecuisine(cuisine):
    cuisines = ['chinese', 'italian', 'indian', 'thai', 'mediterranean']
    return cuisine.lower() in cuisines

def numPeoplevalidate(numPeople):
    numPeople = int(numPeople)
    if numPeople <= 0 or numPeople > 30:
        return False
    else:
        return True

def validaterest(cuisine, numPeople, diningdate, diningtime, location):
    if location is not None and not validatelocation(location):
            return validateall(False, 'Location', 'Please enter valid location')

    if cuisine is not None and not validatecuisine(cuisine):
            return validateall(False, 'Cuisine', 'cuisine not available')
    
    if numPeople is not None and not numPeoplevalidate(numPeople):
            return validateall(False, 'NumberOfPeople', 'too many people or too less people')
            
    if diningdate is not None and not validatedate(diningdate):
            return validateall(False, 'DiningDate', 'future date please')
    
    if diningtime is not None and not validatetime(diningdate, diningtime):
            return validateall(False, 'DiningTime', 'future time please')

    return validateall(True, None, None)

def validatedate(diningdate):
    if datetime.datetime.strptime(diningdate, '%Y-%m-%d').date() < datetime.date.today():
        return False
    else:
        return True

def validatelocation(location):
    return location.isalpha()

def validatetime(diningdate, diningtime):
    if datetime.datetime.strptime(diningdate, '%Y-%m-%d').date() == datetime.date.today():
        if datetime.datetime.strptime(diningtime, '%H:%M').time() <= datetime.datetime.now().time():
            return False
        else:
            return True
    else:
        return True

def getgreetings(intent_request):
    return {
        'dialogAction': {
            "type": "ElicitIntent",
            'message': {
                'contentType': 'PlainText',
                'content': 'Hi there, how can I help?'}
        }
    }

def getthankyou(intent_request):
    return {
        'dialogAction': {
            "type": "ElicitIntent",
            'message': {
                'contentType': 'PlainText',
                'content': 'You are welcome.'}
        }
    }

def getdiningsuggestions(intent_request):
    slots = intent_request['currentIntent']['slots']
    diningdate = slots["DiningDate"]
    diningtime = slots["DiningTime"]
    location = slots["Location"]
    phonenumber = slots["PhoneNumber"]
    cuisine = slots["Cuisine"]
    numPeople = slots["NumberOfPeople"]

    if intent_request['invocationSource'] == 'DialogCodeHook':
        validation_result = validaterest(cuisine, numPeople, diningdate, diningtime, location)
        if not validation_result['isValid']:
            slots[validation_result['violatedSlot']] = None
            return elicit_slot(intent_request['sessionAttributes'], intent_request['currentIntent']['name'], slots, validation_result['violatedSlot'], validation_result['message'])
    
        if intent_request['sessionAttributes'] is not None:
                output = intent_request['sessionAttributes']
        else:
            output = {}
    
        return delegate(output, intent_request['currentIntent']['slots'])
    sendMsg(slots)
    return close(intent_request['sessionAttributes'], 'Fulfilled', {'contentType': 'PlainText', 'content': 'You’re all set. Expect my suggestions shortly! Have a good day.'})
    

def dispatch(intent_request):
    intent_name = intent_request['currentIntent']['name']
    if intent_name == 'DiningSuggestionsIntent':
        return getdiningsuggestions(intent_request)
    elif intent_name == 'GreetingIntent':
        return getgreetings(intent_request)
    elif intent_name == 'ThankYouIntent':
        return getthankyou(intent_request)

    raise Exception('Something went wrong')

def lambda_handler(event, context):
    print(event)

    os.environ['TZ'] = 'America/New_York'
    time.tzset()

    return dispatch(event)