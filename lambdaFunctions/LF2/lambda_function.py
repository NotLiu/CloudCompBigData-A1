import boto3
import json
import random

import requests
from aws_requests_auth.aws_auth import AWSRequestsAuth


def lambda_handler(event, context):
    
    userInput = event['Records'][0]['body'].split("\"")

    # create a boto3 client
    client = boto3.client('sqs')
    ses_client = boto3.client('ses')
    
    dynamoDB = boto3.client('dynamodb', region_name="us-east-1")

    es_host = 'search-restaurant-es-oubiyajdtmmo2jib2vp4q6xmza.us-east-1.es.amazonaws.com'
    region = 'us-east-1'
    service = 'es'

    access_key = 'AKIAVTCRWBNNJO4BMFGF'
    secret_key = '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'
    auth = AWSRequestsAuth(aws_access_key=access_key,
                           aws_secret_access_key=secret_key,
                           aws_host=es_host,
                           aws_region='us-east-1',
                           aws_service='es')

    url = f'https://{es_host}/restaurants/_search'

    location = userInput[3]
    cuisine = userInput[7]
    diningTime = userInput[11]
    peopleCount = userInput[14]
    email = userInput[17]
    
    query = {
        "query": {
            "match": {
                "cuisine": cuisine
            }
        }
    }
    headers = {
        "Content-Type": "application/json"
    }
    response = requests.post(url, auth=auth, headers=headers, json=query)
    
    hits = response.json()['hits']['hits']
    randHits = [hits[random.randint(0,len(hits)-1)]["_source"]["recID"], hits[random.randint(0,len(hits)-1)]["_source"]["recID"], hits[random.randint(0,len(hits)-1)]["_source"]["recID"]]
    
    dbItems = []
    emailParseItems=[]
    count = 0
    for searchHits in randHits:
        count += 1
        item = dynamoDB.get_item(TableName="yelp-restaurants2", Key={'recID':{'S':searchHits}})
        dbItems.append(item)
        emailParseItems.append(str(count)+". "+ item["Item"]["name"]["S"] +", located at "+ item["Item"]["address"]["S"] + ".")
        
    
    email_subject = "Concierge: Restaurant Suggestions"
    email_body = "Hi!\nUsing the information you provided us, we looked hard and found a few great restaurant recommendations for your requirements of:\n\nLocation: " + location + "\nCuisine: " + cuisine + "\nDining Time: "+ diningTime +"\nPeople Count: " + peopleCount + "\n\nHere are the restaurant details!\n\n" + "\n".join(emailParseItems) + "\n\nThanks for using our services! \n\n"

    response = ses_client.send_email(Source="awl323@nyu.edu",
            Destination={'ToAddresses':[email]},
            Message={'Subject':{'Data':email_subject, 'Charset': 'UTF-8'},
                    'Body': {
                        "Text":{
                            'Charset':'UTF-8',
                            'Data': email_body
                        }
                    }
            },

            )
    
    return response