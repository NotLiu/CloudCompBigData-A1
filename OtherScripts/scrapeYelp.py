import boto3
import json
import requests
from decimal import Decimal
import datetime
from time import sleep

dynamoDB = boto3.resource('dynamodb', region_name="us-east-1")
table = dynamoDB.Table('yelp-restaurants2')

API_KEY = "2G-6CTrZWTLy1EGLVrcZTy8i8x50xY3e61QFDqmYV7CZyq60VhcLBJ47zcE6_m_QOz5IUm51FqYcuG0rnxIc59p8-hTsv5qMi1wAA6OR3m3tT3ayaQXFkxxZJCaaZHYx"

CLIENT_ID = "QkxYJ6FdPevJzNwwqQMOmw"

locations = ["Manhattan"]

cuisineTypes = ['American',
                'Chinese',
                'Japanese',
                'Greek',
                'Italian',
                ]

count = 26847


for i in locations:
    print(i)
    for j in cuisineTypes:
        print(j)
        if i == "Manhattan" and j == "American":
            pass
        for thou in range(1):
            url_params = {
                'location': i, 'term': 'restaurant', 'categories': j, 'sort_by': 'best_match', 'limit': 50, "offset": 50*thou
            }

            headers = {'Authorization': 'Bearer 2G-6CTrZWTLy1EGLVrcZTy8i8x50xY3e61QFDqmYV7CZyq60VhcLBJ47zcE6_m_QOz5IUm51FqYcuG0rnxIc59p8-hTsv5qMi1wAA6OR3m3tT3ayaQXFkxxZJCaaZHYx'}
            r = requests.get(
                'https://api.yelp.com/v3/businesses/search', headers=headers, params=url_params)
            print(thou)
            try:
                for x in r.json()['businesses']:

                    item = {
                        'recID': str(count),
                        'businessId': x['id'],
                        'name': x['name'],
                        'cuisineType': j,
                        'location': i,
                        'address': x['location']['address1'],
                        'coordinates': x['coordinates'],
                        'numReviews': x['review_count'],
                        'rating': x['rating'],
                        'zipCode': x['location']['zip_code'],
                        'insertedAtTimestamp': "{:%B %d, %Y}".format(datetime.datetime.now())
                    }

                    item = json.loads(json.dumps(item), parse_float=Decimal)

                    table.put_item(
                        Item=item
                    )
                    sleep(0.001)
                    count += 1
            except:
                print(r.text)


# print(r.json())

# {
#       "id": "kP1b-7BO_VhWk_0tvuA_tw",
#       "alias": "carmelinas-boston-2",
#       "name": "Carmelina's",
#       "image_url": "https://s3-media2.fl.yelpcdn.com/bphoto/rxZBwIYFKwrn2U4676YmiQ/o.jpg",
#       "is_closed": false,
#       "url": "https://www.yelp.com/biz/carmelinas-boston-2?adjust_creative=QkxYJ6FdPevJzNwwqQMOmw&utm_campaign=yelp_api_v3&utm_medium=api_v3_business_search&utm_source=QkxYJ6FdPevJzNwwqQMOmw",
#       "review_count": 3243,
#       "categories": [
#         {
#           "alias": "italian",
#           "title": "Italian"
#         }
#       ],
#       "rating": 4.5,
#       "coordinates": {
#         "latitude": 42.36388,
#         "longitude": -71.05415
#       },
#       "transactions": [
#         "delivery",
#         "restaurant_reservation"
#       ],
#       "price": "$$",
#       "location": {
#         "address1": "307 Hanover St",
#         "address2": "",
#         "address3": "",
#         "city": "Boston",
#         "zip_code": "02113",
#         "country": "US",
#         "state": "MA",
#         "display_address": [
#           "307 Hanover St",
#           "Boston, MA 02113"
#         ]
#       },
#       "phone": "+16177420020",
#       "display_phone": "(617) 742-0020",
#       "distance": 2272.1796574310406
#     },
