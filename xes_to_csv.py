from pm4py.objects.log.importer.xes import factory as xes_import_factory
from pm4py.objects.log.exporter.csv import factory as csv_exporter
import os
import pandas as pd
from pathlib import Path

datasets = ["Helpdesk.xes.gz",
            "BPI_Challenge_2012_W_Complete.xes.gz",
            "bpi_challenge_2013_incidents.xes.gz",
            "BPI_Challenge_2012.xes.gz",
            "BPI_Challenge_2012_Complete.xes.gz",
            "BPI_Challenge_2012_W.xes.gz",
            "BPI_Challenge_2012_A.xes.gz",
            "BPI_Challenge_2012_O.xes.gz",
            "BPI_Challenge_2013_closed_problems.xes.gz",
            "SEPSIS.xes.gz",
            "nasa.xes.gz",
            "env_permit.xes.gz",
            "BPI_2014_Incident_Activity.xes.gz"]

for file in datasets:
    try:
        print("Processing: ", file)
        csv_file = Path(file).stem.split(".")[0] + ".csv"
        log = xes_import_factory.apply("./XES/" + file, parameters={"timestamp_sort": True})
        csv_exporter.export(log, "./CSV/" + csv_file)

        file = pd.read_csv("./CSV/" + csv_file)
        file = file.rename(
            columns={
                "concept:name": "event",
                "case:concept:name": "case",
                "time:timestamp": "completeTime"
            }, errors="raise"
        )

        file.to_csv("./CSV/" + csv_file, sep=",", index=False)
    except OSError:
        print("Unable to process: ", file)
