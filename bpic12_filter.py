from pm4py.objects.log.importer.xes import factory as xes_import_factory
from pm4py.objects.log.exporter.xes import factory as xes_export
from pm4py.objects.log.util import get_log_encoded
from pm4py.algo.filtering.log.attributes import attributes_filter
from pm4py.util import constants

BPIC12_Location = "./XES/BPI_Challenge_2012.xes.gz"
BPIC12_Complete = "./XES/BPI_Challenge_2012_Complete.xes"
BPIC12_W = "./XES/BPI_Challenge_2012_W.xes"
BPIC12_W_Complete = "./XES/BPI_Challenge_2012_W_Complete.xes"
BPIC12_A = "./XES/BPI_Challenge_2012_A.xes"
BPIC12_O = "./XES/BPI_Challenge_2012_O.xes"


def load_xes():
    return xes_import_factory.apply(
        BPIC12_Location,
        variant="iterparse",
        parameters={"timestamp_sort": True, "timestamp_key": "time:timestamp"}
    )


def filter_subprocess(log, subprocess):
    activities = attributes_filter.get_attribute_values(log, "concept:name")
    filtered_activities = [activity for activity in activities if activity.startswith(subprocess)]
    print(filtered_activities)
    filtered_log = attributes_filter.apply_events(log, filtered_activities, parameters={constants.PARAMETER_CONSTANT_ACTIVITY_KEY : "concept:name", "positive" : True})
    return filtered_log

def filter_lifecycle(log):
    filtered_log = attributes_filter.apply_events(
        log,
        ["COMPLETE"],
        # For some reason pm4py understands lifecycle transition as an attribute
        parameters={constants.PARAMETER_CONSTANT_ATTRIBUTE_KEY : "lifecycle:transition", "positive" : True}
    )
    return filtered_log


def print_log(log):
    for trace in log:
        for event in trace:
            print(event["concept:name"], " ->  ", event["lifecycle:transition"])


def save_log(log, directory):
    xes_export.apply(log, directory, parameters={"compress": True})


if __name__ == "__main__":
    bpi_2012 = load_xes()
    print("Filtering W")
    bpi_w = filter_subprocess(bpi_2012, "W")
    print("Filtering W complete")
    bpi_w_complete = filter_lifecycle(filter_subprocess(bpi_2012, "W"))
    print("Filtering complete")
    bpi_complete = filter_lifecycle(bpi_2012)
    print("Filtering A")
    bpi_a = filter_subprocess(bpi_2012, "A")
    print("Filtering O")
    bpi_o = filter_subprocess(bpi_2012, "O")

    print("Saving W")
    save_log(bpi_w, BPIC12_W)
    print("Saving W complete")
    save_log(bpi_w_complete, BPIC12_W_Complete)
    print("Saving complete")
    save_log(bpi_complete, BPIC12_Complete)
    print("Saving A")
    save_log(bpi_a, BPIC12_A)
    print("Saving O")
    save_log(bpi_o, BPIC12_O)
