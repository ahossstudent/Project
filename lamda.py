import base64
import json
import boto3

def lambda_handler(event, context):
  for record in event['Records']:
    
    payload=base64.b64decode(record["kinesis"]["data"])
    
    res = {
      "main_id": str(record['kinesis']['sequenceNumber']),
      "raw_data": str(record),
      "stream_name": str(record['eventSourceARN']),
      "reading": str(payload, 'UTF-8'),
      "approximateArrivalTimestamp": str(record['kinesis']['approximateArrivalTimestamp']) 
    }
    
    write_to_db(res)
    
    print("Object successfully stored in DB.")
    print(record)
       
def write_to_db(data):
  dynamodb = boto3.resource('dynamodb', region_name="us-east-1")
  table = dynamodb.Table("results")

  table.put_item(
      Item=data
  )