import argparse
import datetime

import psutil
import time
import signal
import socket

from typing import TextIO

parser = argparse.ArgumentParser()
parser.add_argument("pid", help="PID of the process to monitor", type=int)
parser.add_argument("task_id", help="ID of the RDataFrame task to monitor", type=int)
parser.add_argument("--label", help="A prefix for the output CSV file")


def mapper_monitor(pid: int, input_file: TextIO, task_id: int) -> None:
    """
    Retrieves some usage metrics from the input process id through psutil.
    """
    global RUN_DISTRDF_MONITOR
    p = psutil.Process(pid)
    while RUN_DISTRDF_MONITOR:
        with p.oneshot():
            metrics = [task_id, p.cpu_percent(), p.memory_info().rss, p.memory_percent(),
                       psutil.net_io_counters().bytes_recv, datetime.datetime.now(), socket.gethostname()]
            input_file.write(f"{','.join((str(metric) for metric in metrics))}\n")
        time.sleep(1)


if __name__ == "__main__":
    args = parser.parse_args()

    RUN_DISTRDF_MONITOR: bool = True

    prefix = args.label if args.label is not None else "distrdf"
    outpath = f"{prefix}_monitor_{args.task_id}.csv"

    csvfile = open(outpath, "w")

    header = ["task_id",
              "cpu_percent",
              "memory_rss",
              "memory_percent_rss",
              "net_read",
              "timestamp",
              "hostname"]
    csvfile.write(f"{','.join(header)}\n")

    def sigterm_handler(_signal_number, _stack_frame):
        global RUN_DISTRDF_MONITOR
        RUN_DISTRDF_MONITOR = False
        csvfile.close()

    signal.signal(signal.SIGTERM, sigterm_handler)

    mapper_monitor(args.pid, csvfile, args.task_id)
