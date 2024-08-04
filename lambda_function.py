import csv
import json
import boto3
import time
from datetime import datetime


def lambda_handler(event, context):
    
    event_params=event["Records"][0]
    
    bucket=event_params["s3"]["bucket"]["name"]
    key=event_params["s3"]["object"]["key"]
    
    print("here")
    
    s3_resource = boto3.resource('s3')
    s3_object = s3_resource.Object(bucket, key)
    
    data = s3_object.get()['Body'].read().decode('utf-8').splitlines()
    
    lines = csv.reader(data)
    print(lines)
    headers = next(lines)
    print('headers: %s' %(headers))
    
    list_data = list(lines)
    
    print(list_data)
    india=[]
    us=[]
    for i in list_data:
        if i[3]=='India':
            india.append(int(i[2]))
        else:
            us.append(int(i[2]))
        
    print('total india salary spend is ',sum(india))
    print('total india salary spend is ',sum(us))
    print(f"""total india,us salary spend is ,{sum(india),sum(us)}""")
    file_content=f"""total india,us salary spend is ,{sum(india),sum(us)}"""
    if key=='employee.csv':
        s3 = boto3.client('s3') 
        s3.put_object(Body=file_content, Bucket=bucket, Key='agg')
    
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }