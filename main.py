import datetime
import json
import os
from re import M

import pandas as pd
from dotenv import load_dotenv

from pdf_generator import PdfGenerator
from rds_writer import RdsWriter
from s3_helper import S3Helper
from timestream_reader import TimestreamReader


def get_envs():
    load_dotenv()

    return {
        'aws_region':  os.environ['AWS_REGION'],
        'aws_access_key': os.environ['AWS_ACCESS_KEY'],
        'aws_secret_key': os.environ['AWS_SECRET_KEY'],
        'timestream': {
            'db': os.environ['AWS_TIMESTREAM_DB'],
            'table': os.environ['AWS_TIMESTREAM_TABLE'],
        },
        'rds': {
            'endpoint': os.environ['AWS_RDS_ENDPOINT'],
            'port': os.environ['AWS_RDS_PORT'],
            'db': os.environ['AWS_RDS_DB'],
            'user': os.environ['AWS_RDS_USER'],
            'password': os.environ['AWS_RDS_PASSWORD'],
            'elaboration_table': os.environ['AWS_RDS_ELABORATION_TABLE'],
        },
        's3': {
            'bucket': os.environ['AWS_S3_BUCKET'],
            'reports_folder': os.environ['AWS_S3_FOLDER_REPORT'],
        },
        'outputFile': os.environ['OUTPUT_FILE']
    }

if __name__ == '__main__':

    config = get_envs()

    tr = TimestreamReader(
        access_key=config['aws_access_key'],
        secret_key=config['aws_secret_key'],
        database=config['timestream']['db'],
        table=config['timestream']['table'],
    )

    rds_client = RdsWriter(
        db=config['rds']['db'],
        endpoint=config['rds']['endpoint'],
        port=config['rds']['port'],
        user=config['rds']['user'],
        password=config['rds']['password'],
    )

    pg = PdfGenerator(config['outputFile'])

    s3_helper = S3Helper(
        access_key=config['aws_access_key'],
        secret_key=config['aws_secret_key'],
        region=config['aws_region'],
        bucket=config['s3']['bucket'],
        folder=config['s3']['reports_folder'],
    ) 

    data = tr.get_timestream_data()

    df = pd.DataFrame(data)

    data_ingested_today = len(df)
    falls = 0
    avg_serendipity = 0
    location_density = []

    if data_ingested_today == 0:
        falls = int(df["nFall"].sum().item())
        avg_serendipity = int(df['serendipity'].mean().item())
        grouped_by_coords = df.groupby(['latitude', 'longitude']).size().reset_index(name='total').sort_values(by=['total'])
        grouped_by_coords.apply(lambda x: location_density.append(json.loads(x.to_json())), axis=1)
    
    
    # write to database
    rds_client.write_elaborated_data(
        config['rds']['elaboration_table'],
        data_ingested_today,
        falls,
        avg_serendipity,
        location_density
    )


    # generate report pdf
    pg.generate_report_pdf({
        'id': datetime.date.today(),
        'generated_at': datetime.date.today(),
        'data_ingested': data_ingested_today,
        'falls': falls,
        'serendipity': avg_serendipity
    })

    # upload report to s3
    s3_helper.upload_pdf_report(config['outputFile'])


    pg.delete_report()
    

