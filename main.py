from timestream_reader import TimestreamReader


if __name__ == '__main__':
    tr = TimestreamReader()

    c, r = tr.get_timestream_data_in_csv()
