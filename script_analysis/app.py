
import os

from analysis import Analysis


def get_envs():

    return {
        'aws_region':  os.environ['REGION'],
        'aws_access_key': os.environ['ACCESS_KEY_ID'],
        'aws_secret_key': os.environ['SECRET_ACCESS_KEY'],
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
    print('Started Analysis Lambda')
    try:
        print('Get Configuration')
        print(config)
        config = get_envs()
        print('Start Analyze')
        Analysis.analyze(config)
        print('Finish Analyzing')
    except Exception as e:
        print('Script analysis failed. Error:')
        print(e)
