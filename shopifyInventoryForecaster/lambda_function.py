import json
import boto3
from inventory_forecaster.forecaster import main


client = boto3.client('ses', region_name='us-west-1')

body_text = main()

def lambda_handler(event, context):
    try:
        response = client.send_email(
            Destination={
                'ToAddresses': ['cris@valleplateado.com']
            },
            Message={
                'Body': {
                    'Text': {
                        'Charset': 'UTF-8',
                        'Data': body_text,
                    }
                },
                'Subject': {
                    'Charset': 'UTF-8',
                    'Data': 'Test email',
                },
            },
            Source='info@valleplateado.com'
        )

        print(response)

        return {
            'statusCode': 200,
            'body': json.dumps("Email Sent Successfully. MessageId is: " + response['MessageId'])
        }
    except Exception as e:
        return str(e)