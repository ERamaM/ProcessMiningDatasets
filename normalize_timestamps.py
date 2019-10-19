import pandas

# Event log to be normalized
file = "bpic12_work_complete.csv"

# Timestamp columns to be normalized
timestamp_columns = ["startTime", "completeTime"]

# Desired output timestamp format
#timestamp_format = "%Y-%m-%dT%H:%M:%S.000-07:00"
timestamp_format = "%Y-%m-%d %H:%M:%S"

df = pandas.read_csv(file, header=0, delimiter=",")
for timestamp_column in timestamp_columns:
    df[timestamp_column] = pandas.to_datetime(df[timestamp_column])
    df[timestamp_column] = df[timestamp_column].dt.strftime(timestamp_format)

df.to_csv(file, index=False)


