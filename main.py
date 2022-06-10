import json
import boto3
import pprint
from botocore.config import Config
import time

session = boto3.Session()

def write_data():

    write_client = session.client(
        'timestream-write', 
        config=Config(read_timeout=20, max_pool_connections = 5000, retries={'max_attempts': 10})
    )

    data = {
        'Dimensions': [
            {
                'Name': 'data',
                'Value': 'bracelet_data'
            }
        ],
        'Time': str(round(time.time() * 1000)),
        'MeasureName': 'braceletData',
        'MeasureValue' : json.dumps(
            {
                'uid': '0038f08e-9545-4fca-a3a7-9ea9b409cdf8',
                'data': {
                    'serendipity': 70,
                    'steps': 500,
                    'heartbeat': 60,
                    'nFall': 0,
                    'battery': 70,
                    'standing': 5,
                }
            }
        ),
        'MeasureValueType': 'VARCHAR'
    }

    try:
        write_client.write_records(
            DatabaseName='clod2021_ProjectWork_G3',
            TableName='bracelet_data',
            Records=[data]
        )
    except Exception as err:
        print(err)


def read():
    client = session.client('timestream-query')

    result = client.query(
        QueryString='SELECT * FROM "clod2021_ProjectWork_G3"."bracelet_data"'
    )

    pprint.pprint(result)


if __name__ == '__main__':
    read()

