import boto3
import json

# Define the client to interact with Lex
def lambda_handler(event, context):
    client = boto3.client('sqs')
    if "Phone_number" in event['interpretations'][0]['intent']['slots']:
        phone_number = event['interpretations'][0]['intent']['slots']['Phone_number']['value']['interpretedValue']
        cuisine = event['interpretations'][0]['intent']['slots']['Cuisine']['value']['interpretedValue']
        number_of_people = event['interpretations'][0]['intent']['slots']['Number_of_people']['value']['interpretedValue']
        dining_time = event['interpretations'][0]['intent']['slots']['Dining_Time']['value']['interpretedValue']
        location = event['interpretations'][0]['intent']['slots']['Location']['value']['interpretedValue']
        date = event['interpretations'][0]['intent']['slots']['Date']['value']['interpretedValue']
        
        
        res_to_sqs = {
            'phone_number': phone_number,
            'cuisine': cuisine,
            'number_of_people': number_of_people,
            'dining_time': dining_time,
            'location': location,
            'date': date
        }
        
        response = client.send_message(
            QueueUrl="https://sqs.us-east-1.amazonaws.com/497974258925/DiningConciergeQueue",
            MessageBody=json.dumps(res_to_sqs)
        )
        print(event)
        return {  
                "sessionState": {
                    "dialogAction": {
                        "type": "Close",
                    },
                    "intent": {
                        "name": "DiningSuggestionsIntent",
                        "state": "Fulfilled"
                    }
                },
                "messages": [
                    {
                        "contentType": "PlainText",
                        "content": "You’re all set. Expect my suggestions shortly! Have a good day."
                    }
                ]
            }
    
            