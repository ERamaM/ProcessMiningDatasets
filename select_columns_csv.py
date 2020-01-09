import pandas as pd
import glob
import os

csv_dir = "./CSV/"
output_dir = "./CSV/preprocessed/"
file = "bpi_2014.csv"

case_column = "case"
activity_column = "event"
timestamp_column = "completeTime"
timestamp_format = "%Y-%m-%d %H:%M:%S"

for csv in glob.iglob(csv_dir + "*.csv"):

    print(csv)
    dataset = pd.read_csv(csv)

    dataset = dataset[[case_column, activity_column, timestamp_column]]
    dataset[timestamp_column] = pd.to_datetime(dataset[timestamp_column], utc=True)
    dataset[timestamp_column] = dataset[timestamp_column].dt.strftime(timestamp_format)


    dataset[case_column] = dataset[case_column].astype("category").cat.codes
    dataset[activity_column] = dataset[activity_column].astype("category").cat.codes

    dataset = dataset.rename(
    columns={
        case_column: "CaseID",
        activity_column: "ActivityID",
        timestamp_column: "CompleteTimestamp"
    }, errors="raise"
    )

    dataset.to_csv(output_dir + os.path.basename(csv), sep=",", index=False)
