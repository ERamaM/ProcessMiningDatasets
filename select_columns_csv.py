import pandas as pd

csv_file = "./CSV/bpi_2014.csv"

case_column = "case"
activity_column = "event"
timestamp_column = "completeTime"
timestamp_format = "%Y-%m-%d %H:%M:%S"

file = pd.read_csv(csv_file)

dataset = file[[case_column, activity_column, timestamp_column]]
dataset[timestamp_column] = pd.to_datetime(dataset[timestamp_column])
dataset[timestamp_column] = dataset[timestamp_column].dt.strftime(timestamp_format)
d = dataset.rename(
    columns={
        case_column: "CaseID",
        activity_column: "ActivityID",
        timestamp_column: "CompleteTimestamp"
    }, errors="raise"
)
d.to_csv(csv_file, sep=",", index=False)
