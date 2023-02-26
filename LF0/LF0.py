import boto3
import json

# Define the client to interact with Lex
client = boto3.client('lexv2-runtime')
def lambda_handler(event, context):
    print("this is event", event)
    msg_from_user = json.loads(event['body'])['messages'][0]['unstructured']['text']
    print(f'Message from frontend: {msg_from_user}')

    # Initiate conversation with Lex
    response = client.recognize_text(
            botId='FMLXI1S9W9', # MODIFY HERE
            botAliasId='A2DY9C1MQG', # MODIFY HERE
            localeId='en_US',
            sessionId='testuser',
            text=msg_from_user)
    
    msg_from_lex = response.get('messages', [])
    if msg_from_lex:
        
        print(f"Message from Chatbot: {msg_from_lex[0]['content']}")
        print(response)
        resp = {
            'statusCode': 200,
            'headers': {
            'Access-Control-Allow-Headers': 'Content-Type',
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'POST'
            },
            'body': json.dumps({
              'messages': [
                {
                "type": "unstructured",
                "unstructured": {
                  "text": msg_from_lex[0]['content']
                }
              }
            ]
          })

        }
        # modify resp to send back the next question Lex would ask from the user
        
        # format resp in a way that is understood by the frontend
        # HINT: refer to function insertMessage() in chat.js that you uploaded
        # to the S3 bucket
        return resp
        