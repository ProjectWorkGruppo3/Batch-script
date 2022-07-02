
import datetime
import json

import pandas as pd

from pdf_generator import PdfGenerator
from rds_writer import RdsWriter
from s3_helper import S3Helper
from timestream_reader import TimestreamReader


class Analysis:

    @staticmethod
    def analyze(config):
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
            falls = int(df["numberOfFalls"].sum().item())
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
