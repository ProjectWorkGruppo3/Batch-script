import json
import os

import pandas as pd
from dotenv import load_dotenv

from rds_writer import RdsWriter
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
        }
    }

def get_rds_client(config):
    return RdsWriter(
        db=config['rds']['db'],
        endpoint=config['rds']['endpoint'],
        port=config['rds']['port'],
        user=config['rds']['user'],
        password=config['rds']['password'],
    )

if __name__ == '__main__':

    config = get_envs()

    tr = TimestreamReader(
        access_key=config['aws_access_key'],
        secret_key=config['aws_secret_key'],
        database=config['timestream']['db'],
        table=config['timestream']['table'],
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
    
    rds_client = get_rds_client(config)

    rds_client.write_elaborated_data(
        config['rds']['elaboration_table'],
        data_ingested_today,
        falls,
        avg_serendipity,
        location_density
    )

    

    

