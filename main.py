
from timestream_reader import TimestreamReader
import pandas as pd

if __name__ == '__main__':
    tr = TimestreamReader()

    data = tr.get_timestream_data()

    df = pd.DataFrame(data)

    print(df.head())

