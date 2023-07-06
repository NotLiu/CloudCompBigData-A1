import json
import boto3

lex = boto3.client('lexv2-runtime')

#call lex function
def lambda_handler(event, context):
    print(event)
    
    user_input = event['messages'][0]['unstructured']['text']
    bot_id = 'IXFZCDSI92'
    bot_alias_id = 'TSTALIASID'
    locale_id = 'en_US'
    
    response = lex.recognize_text(
        botId = bot_id,
        botAliasId = bot_alias_id,
        localeId = locale_id,
        sessionId = 'abc1234',
        text = user_input
        )
    
    conciergeMsg = ""
    if response['messages'] is not None:
        conciergeMsg = response['messages']
    
    return {
        'statusCode': 200,
        'body': conciergeMsg}
