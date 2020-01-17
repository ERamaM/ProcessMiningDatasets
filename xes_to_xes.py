"""
Sort the traces of a XES file by its timestamp.
"""
from pm4py.objects.log.importer.xes import factory as xes_import_factory
from pm4py.objects.log.exporter.xes import factory as xes_exporter
import os
import pandas as pd

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
    print("Processing: ", file)
    log = xes_import_factory.apply("./XES/" + file, parameters={"timestamp_sort": True})
    name = file.split(".")[0]
    xes_exporter.export_log(log, "./XES/ordered/" + name + ".xes", parameters={"compress" : True})
