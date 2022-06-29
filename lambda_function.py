
import os

from dotenv import load_dotenv

from analysis import Analysis


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

def lambda_handler(event, context):
    try:
        config = get_envs()
        Analysis.analyze(config)
    except Exception as e:
        print('Script analysis failed. Error:')
        print(e)

