import pandas as pd
import glob
import os

csv_dir = "./CSV/customsort/"
output_dir = "./CSV/preprocessed_pmrec_customsort/"
file = "bpi_2014.csv"

case_column = "case:concept:name"
activity_column = "concept:name"
timestamp_column = "time:timestamp"
lifecycle_column = "lifecycle:transition"
timestamp_format = "%Y-%m-%d %H:%M:%S"

for dataset_name in glob.iglob(csv_dir + "*"):
    if not os.path.exists(output_dir):
        os.mkdir(output_dir)
    dataset = pd.read_csv(dataset_name)

    if lifecycle_column is None:
        dataset = dataset[[case_column, activity_column, timestamp_column]]
    else:
        dataset = dataset[[case_column, activity_column, timestamp_column, lifecycle_column]]
    dataset[timestamp_column] = pd.to_datetime(dataset[timestamp_column], utc=True)
    dataset[timestamp_column] = dataset[timestamp_column].dt.strftime(timestamp_format)


    dataset[case_column] = dataset[case_column].astype("category").cat.codes
    dataset[activity_column] = dataset[activity_column].astype("category").cat.codes

    if lifecycle_column is None:
        dataset = dataset.rename(
        columns={
            case_column: "CaseID",
            activity_column: "ActivityID",
            timestamp_column: "CompleteTimestamp"
        }
        )
    else:
        dataset = dataset.rename(
            columns={
                case_column: "caseId",
                activity_column: "activity",
                timestamp_column: "timestamp",
                lifecycle_column: "lifecycle"
            }
        )

    dataset.to_csv(output_dir + os.path.basename(dataset_name), sep=",", index=False)
