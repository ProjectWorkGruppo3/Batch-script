from dotenv import load_dotenv
import os
from timestream_reader import TimestreamReader
import pandas as pd



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
        }
    }


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

