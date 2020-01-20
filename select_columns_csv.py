import pandas as pd
import glob
import os

csv_dir = "./CSV/splits/"
output_dir = ""
file = "bpi_2014.csv"

case_column = "case:concept:name"
activity_column = "concept:name"
timestamp_column = "time:timestamp"
timestamp_format = "%Y-%m-%d %H:%M:%S"

for dataset in glob.iglob(csv_dir + "*"):
    print("Dataset: " + dataset)
    preprocessed_dir = dataset.replace("splits", "preprocessed")
    if not os.path.exists(preprocessed_dir):
        os.mkdir(preprocessed_dir)
    output_dir = preprocessed_dir + "/"
    for csv in glob.iglob(dataset + "/*.csv"):

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
        }
        )

        dataset.to_csv(output_dir + os.path.basename(csv), sep=",", index=False)
