from pm4py.objects.log.importer.xes import factory as xes_import_factory
from pm4py.objects.log.exporter.csv import factory as csv_exporter
import os
import pandas as pd


xes_file = "./XES/bpi_challenge_2013_incidents.xes.gz"
csv_file = "bpi_2013_incidents.csv"
filename = os.path.basename(xes_file)

log = xes_import_factory.apply(xes_file, parameters={"timestamp_sort" : True})
csv_exporter.export(log, "./CSV/" + csv_file)

file = pd.read_csv("./CSV/" + csv_file)
file = file.rename(
    columns={
        "concept:name" : "event",
        "case:concept:name" : "case",
        "time:timestamp" : "completeTime"
    }, errors="raise"
)

file.to_csv("./CSV/" + csv_file, sep=",", index=False)


