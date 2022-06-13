import boto3
import csv
import json

session = boto3.Session()

def read():
    client = session.client('timestream-query')

    
    result = client.query(
        QueryString='SELECT * FROM "clod2021_ProjectWork_G3"."bracelet_data"'
    )

    raw_rows = result['Rows']
    raw_columns = result['ColumnInfo']
    columns = get_columns(raw_columns)
    
    time_index = columns.index('time')
    data_index = columns.index('measure_value::varchar')

    data = get_data(raw_rows)
    time_data = []
    for d in data:
        time = d[time_index]
        json_data = d[data_index]

        time_data.append({
            'time': time,
            'json': json_data
        })

    mapped = []
    for td in time_data:
        r = {
            'time': td['time']
        }
        dj = json.loads(td['json'])

        for k, v in dj.items():
            r[k] = v # uid or data
        
        mapped.append(r)

    csv = []
    csv_columns = ['time', 'uid']

    cc = []
    for m in mapped:
        csv_row = [m['time'], m['uid']]
        bracelet_data = m['data']
        keys = []

        for k, v in bracelet_data.items():
            csv_row.append(v)
            keys.append(k)
            
        cc = list(set(cc) | set(keys))

        
        csv.append(csv_row)
    
    
    write_csv(csv_columns + cc, csv)



def get_columns(raw_columns):
    columns = []
    
    for c in raw_columns:
        columns.append(c['Name'])

    return columns

def get_data(raw_data):
    data = []

    for rd in raw_data:
        row = []
        for d in rd['Data']:
            row.append(list(d.values())[0])
        data.append(row)

    return data


def write_csv(columns, data):
    with open('data.csv', 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(columns)
        writer.writerows(data)

if __name__ == '__main__':
    read()

