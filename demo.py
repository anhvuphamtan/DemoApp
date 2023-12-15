from datetime import datetime
import pandas as pd
import dotenv
import socket
import faker
import boto3
import os

def setVarGlobal():
    dotenv.load_dotenv()
    global AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, REGION, BUCKET_NAME, BUCKET_KEY, HOST_NAME
    AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
    AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
    REGION = os.getenv('REGION')
    BUCKET_NAME = os.getenv('BUCKET_NAME')
    BUCKET_KEY  = os.getenv('BUCKET_KEY')
    HOST_NAME   = os.getenv('HOST_NAME')

def readDataFromS3():
    S3_client = boto3.client(
        "s3",
        aws_access_key_id = AWS_ACCESS_KEY_ID,
        aws_secret_access_key = AWS_SECRET_ACCESS_KEY,
        region_name = REGION 
    )

    response = S3_client.get_object(
        Bucket = BUCKET_NAME,
        Key    = BUCKET_KEY
    )

    data = response['Body']

    return data

def processingData(data):
    cnt = 0
    for line in data: 
        cnt += 1
    
    with open(f"{socket.gethostname()}.log", "w") as file:
        file.write(f"{socket.gethostname()} - {datetime.now()} - Found {cnt} records in {BUCKET_KEY}")
    
def main():
    data = readDataFromS3()
    processingData(data)

if __name__ == "__main__":
    setVarGlobal()
    main()