from datetime import datetime

import boto3


class S3Helper:

    def __init__(self, access_key, secret_key, region, bucket, folder):
        self.client = boto3.client(
            's3',
            aws_access_key_id=access_key, 
            aws_secret_access_key=secret_key, 
            region_name=region
        )
        self.bucket = bucket
        self.folder = folder
    
    def upload_pdf_report(self, filepath):
        """ Upload report to s3"""
        self.client.upload_file(
            filepath, 
            self.bucket, 
            f'{self.folder}/report_{datetime.now().isoformat()}.pdf'
        )
