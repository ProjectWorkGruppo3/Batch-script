import boto3
from datetime import datetime

class S3Helper:

    def __init__(self, access_key, secret_key, region, bucket):
        self.client = boto3.client(
            's3',
            aws_access_key_id=access_key, 
            aws_secret_access_key=secret_key, 
            region_name=region
        )
        self.bucket = bucket
    
    def upload_pdf_report(self, data, report_name=None):
        """ Upload report to s3"""
        self.client.upload_fileobj(
            data, 
            self.bucket, 
            report_name if report_name != None else f'report_{datetime.now().isoformat()}'
        )