import datetime
import json

import boto3
import psycopg2


class RdsWriter:
    def __init__(
        self, 
        endpoint,
        port,
        db,
        user,
        password
    ):
        self.endpoint = endpoint
        self.port = port
        self.db = db
        self.user = user
        self.password = password
    
    def write_elaborated_data(
        self, 
        table, 
        data_ingested,
        falls,
        serendipity,
        location_density
    ):
        """Write data analyzed"""

        connection = psycopg2.connect(
            host=self.endpoint,
            port=self.port,
            database=self.db,
            user=self.user,
            password=self.password
        )
        connection.set_session(autocommit=True)

        cursor = connection.cursor()

        cursor.execute(
            f'''
                insert into public."{table}" ("Date", "DataIngested", "Falls", "Serendipity", "LocationDensity") 
                values (%s, %s, %s, %s, %s)
            ''',
            (
                datetime.datetime.now(), 
                data_ingested,
                falls,
                serendipity,
                json.dumps(location_density)
            )
        )

        connection.close()
