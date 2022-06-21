import json
from dotenv import load_dotenv
import os
from timestream_reader import TimestreamReader
import pandas as pd

from rds_writer import RdsWriter

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

def write_to_db(config, data):
    rds = RdsWriter(
        db=config['rds']['db'],
        endpoint=config['rds']['endpoint'],
        port=config['rds']['port'],
        user=config['rds']['user'],
        password=config['rds']['password'],
    )

    rds.write_elaborated_data(
        table=config['rds']['elaboration_table'],
        data=data
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

    print(df.head())

    # Number of data ingested today - number
    data_ingested_today = len(df)
    print(f'Data ingested today: {data_ingested_today}')
    

    # - Avarage Total Number of Falls - number
    avg_n_falls = df["nFall"].mean()
    print(f'Avarage N. Falls {avg_n_falls}') 

    # - Total Bracelets - number
    n_devices = len(df.groupby('device_id').size().reset_index(name='total')['device_id'])
    print(n_devices)

    

    # - Scatter Chart / Google Map Density  Location of the bracelets TODO when put the position
    # type of data to return array of this object
    # {
    #     city: string,
    #     coordinates: {
    #         latitude: number,
    #         longitude: number,
    #     },
    #     totalDevices: number;   
    # }

    
    # New devices today TODO
    
    # Avarage serendipity TODO
    avg_serendipity = df['serendipity'].mean()
    print(f"Avarage serendipity: {avg_serendipity}")
    
    



    # ANCHOR
    # list_conv = []
    # df.apply(lambda x: list_conv.append(json.loads(x.to_json())), axis=1)


    # write_to_db(config, list_conv)

    

    

