import json
import boto3
from botocore.exceptions import ClientError
from decimal import Decimal
import requests

def lambda_handler(event, context):
    # uni is the primary/paritition key
    # note they all have unique attributes
    
    headers = {
        "accept": "application/json",
        "Authorization": "Bearer lxrBkAp1X_ohoOUJIRt_PERE7dtMvmvtdY6pBQFAml6T61LYVsPzIf4WSiHtdUc9LYd0OLeVM3LiFrsPSV8f0Pfnnk9kf_ZZbKC721OSVLghPhMdLADZtU1iX6r7Y3Yx"
    }
    
    cuisines = ["Korean", "Brazilian", "French", "Thai", "Greek", "Indian", "Japanese", "Mexican", "Chinese", "Italian"]
    locations = ["Queens", "Brooklyn", "The Bronx", "Staten Island", "Manhattan"]
    limit = 50
    for cuisine in cuisines:
        for location in locations:
            offset = 0
            while offset <= 1500:
                url = f'https://api.yelp.com/v3/businesses/search?location={location}&term=restaurants&categories={cuisine}&sort_by=best_match&limit=50&offset={offset}'
                response = requests.get(url, headers=headers)
                if len(json.loads(response.text)) == 0 or "businesses" not in json.loads(response.text): break
                insert_data(scrape_data(json.loads(response.text), cuisine))
                if len(json.loads(response.text)["businesses"]) < limit:
                    break
                offset+=limit
    # 1
    

def scrape_data(data, cuisine):
    data_to_add = []
    for restaurant in data["businesses"]:
        data_to_add.append({
            "id": restaurant["id"],
            "name": restaurant["name"],
            "address": ', '.join(restaurant["location"]["display_address"]),
            "coordinates": {
                "latitude": str(restaurant["coordinates"]["latitude"]),
                "longitude": str(restaurant["coordinates"]["longitude"])
            },
            "review_count": restaurant["review_count"],
            "rating": restaurant["rating"],
            "zip_code": restaurant["location"]["zip_code"],
            "cuisine": cuisine
        })
    data_to_add = json.loads(json.dumps(data_to_add), parse_float=Decimal)
    return data_to_add
        
    # print(data_to_add)
    
    # insert_data(data_to_add)
    # 2
    # lookup_data({'uni': 'xx777'})
    # 3
    # update_item({'uni': 'xx777'}, 'Canada')
    # 4
    # delete_item({'uni': 'xx777'})
    return
def insert_data(data_list, db=None, table='yelp-restaurants'):
    if not db:
        db = boto3.resource('dynamodb')
    table = db.Table(table)
    # overwrite if the same index is provided
    for data in data_list:
        response = table.put_item(Item=data)
    print('@insert_data: response', response)
    return response
def lookup_data(key, db=None, table='yelp-restaurants'):
    if not db:
        db = boto3.resource('dynamodb')
    table = db.Table(table)
    try:
        response = table.get_item(Key=key)
    except ClientError as e:
        print('Error', e.response['Error']['Message'])
    else:
        print(response['Item'])
        return response['Item']
def update_item(key, feature, db=None, table='yelp-restaurants'):
    if not db:
        db = boto3.resource('dynamodb')
    table = db.Table(table)
    # change student location
    response = table.update_item(
        Key=key,
        UpdateExpression="set #feature=:f",
        ExpressionAttributeValues={
            ':f': feature
        },
        ExpressionAttributeNames={
            "#feature": "from"
        },
        ReturnValues="UPDATED_NEW"
    )
    print(response)
    return response
def delete_item(key, db=None, table='yelp-restaurants'):
    if not db:
        db = boto3.resource('dynamodb')
    table = db.Table(table)
    try:
        response = table.delete_item(Key=key)
    except ClientError as e:
        print('Error', e.response['Error']['Message'])
    else:
        print(response)
        return response
        
        
