from pm4py.objects.log.importer.csv import factory as csv_import_factory
from pm4py.objects.conversion.log import factory as conversion_factory
from pm4py.objects.log.exporter.csv import factory as csv_exporter
from pm4py.objects.log.exporter.xes import factory as xes_exporter

import random
import pandas as pd
import os

datasets = ["Helpdesk.csv",
            "BPI_Challenge_2012_W_Complete.csv",
            "bpi_challenge_2013_incidents.csv",
            "BPI_Challenge_2012.csv",
            "BPI_Challenge_2012_Complete.csv",
            "BPI_Challenge_2012_W.csv",
            "BPI_Challenge_2012_A.csv",
            "BPI_Challenge_2012_O.csv",
            "BPI_Challenge_2013_closed_problems.csv",
            "SEPSIS.csv",
            "nasa.csv",
            "env_permit.csv",
            "BPI_2014_Incident_Activity.csv"]

def check_directories(dataset):
    if not os.path.exists("./CSV/splits/"):
        os.mkdir("./CSV/splits")
    if not os.path.exists("./CSV/customsort/"):
        os.mkdir("./CSV/customsort")
    if not os.path.exists("./XES/splits/"):
        os.mkdir("./XES/splits")
    if not os.path.exists("./XES/customsort/"):
        os.mkdir("./XES/customsort/")
    if not os.path.exists("./XES/splits/" + dataset):
        os.mkdir("./XES/splits/" + dataset)
    if not os.path.exists("./CSV/splits/" + dataset):
        os.mkdir("./CSV/splits/" + dataset)

for dataset in datasets:

    print("Processing: ", dataset)
    pandas_df = csv_import_factory.import_dataframe_from_path("./CSV/" + dataset, parameters={"timest_columns" : "time:timestamp"})
    # Sort the dataframe
    groups = [pandas_df for _, pandas_df in pandas_df.groupby("case:concept:name")]
    random.seed(42)
    random.shuffle(groups)

    train_size = round(len(groups) * 0.64)
    val_size = round(len(groups) * 0.8)

    train_groups = groups[:train_size]
    val_groups = groups[train_size:val_size]
    test_groups = groups[val_size:]
    train_val_groups = groups[:val_size]

    train = pd.concat(train_groups).reset_index(drop=True)
    val = pd.concat(val_groups).reset_index(drop=True)
    test = pd.concat(test_groups).reset_index(drop=True)
    train_val = pd.concat(train_val_groups).reset_index(drop=True)
    full = pd.concat(groups).reset_index(drop=True)


    train_log = conversion_factory.apply(train)
    val_log = conversion_factory.apply(val)
    test_log = conversion_factory.apply(test)
    train_val_log = conversion_factory.apply(train_val)
    full_log = conversion_factory.apply(full)

    check_directories(dataset)

    csv_exporter.apply(train_log, "./CSV/splits/" + dataset + "/train_" + dataset, parameters={"timest_columns" : "time:timestamp"})
    csv_exporter.apply(val_log, "./CSV/splits/" + dataset + "/val_" + dataset, parameters={"timest_columns" : "time:timestamp"})
    csv_exporter.apply(test_log, "./CSV/splits/" + dataset + "/test_" + dataset, parameters={"timest_columns" : "time:timestamp"})
    csv_exporter.apply(train_val_log, "./CSV/splits/" + dataset + "/train_val_" + dataset, parameters={"timest_columns" : "time:timestamp"})
    csv_exporter.apply(full_log, "./CSV/customsort/" + dataset, parameters={"timest_columns" : "time:timestamp"})

    xes_exporter.apply(train_log, "./XES/splits/" + dataset + "/train_" + dataset.split(".")[0] + ".xes", parameters={"timest_columns" : "time:timestamp", "compress" : True})
    xes_exporter.apply(val_log, "./XES/splits/" + dataset + "/val_" + dataset.split(".")[0] + ".xes", parameters={"timest_columns" : "time:timestamp", "compress" : True})
    xes_exporter.apply(test_log, "./XES/splits/" + dataset + "/test_" + dataset.split(".")[0] + ".xes", parameters={"timest_columns" : "time:timestamp", "compress" : True})
    xes_exporter.apply(train_val_log, "./XES/splits/" + dataset + "/train_val_" + dataset.split(".")[0] + ".xes", parameters={"timest_columns" : "time:timestamp", "compress" : True})
    xes_exporter.apply(full_log, "./XES/customsort/" + dataset.split(".")[0] + ".xes", parameters={"timest_columns" : "time:timestamp", "compress" : True})

