import json
import boto3
from time import sleep

dynamoDB = boto3.resource('dynamodb', region_name="us-east-1")
table = dynamoDB.Table('yelp-restaurants2')

response = table.scan()
data = response['Items']

f = open("data.ndjson", "w")

count = 0
for i in data:
    index = {"index": {"_index": "restaurants", "_id": str(count)}}
    indexData = {"recID": i['recID'],
                 "businessID": i['businessId'],
                 "cuisine": i['cuisineType']}

    # print(index)
    # print(indexData)
    # print("=====================")
    f.write(json.dumps(index))
    f.write("\n")
    f.write(json.dumps(indexData))
    f.write("\n")

    count += 1

f.close()
