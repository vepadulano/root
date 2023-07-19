#  @author Vincenzo Eduardo Padulano
#  @author Enric Tejedor
#  @date 2021-11

################################################################################
# Copyright (C) 1995-2021, Rene Brun and Fons Rademakers.                      #
# All rights reserved.                                                         #
#                                                                              #
# For the licensing terms see $ROOTSYS/LICENSE.                                #
# For the list of contributors see $ROOTSYS/README/CREDITS.                    #
################################################################################
from __future__ import annotations

import os
from typing import Any, Dict, Optional, TYPE_CHECKING
import time
import random
import copy
import math
from typing import List

import ROOT

from DistRDF import DataFrame
from DistRDF import HeadNode
from DistRDF.Backends import Base
from DistRDF.Backends import Utils

try:
    import dask
    from dask.distributed import Client, get_worker, LocalCluster, progress, as_completed
except ImportError:
    raise ImportError(("cannot import a Dask component. Refer to the Dask documentation "
                       "for installation instructions."))

if TYPE_CHECKING:
    from dask_jobqueue import JobQueueCluster


def live_visualize(histograms: List[ROOT.TH1D]) -> None:
    """
    Enables live visualization for the given histograms by setting the
    live_visualization_enabled flag of the Headnode to True.

    Args:
        histograms (List[ROOT.TH1D]): The list of histograms to enable live visualization for.
    """
    headnode = histograms[0].proxied_node.get_head()
    headnode.live_visualization_enabled = True
    headnode.histogram_ids = [histogram.proxied_node.node_id for histogram in histograms]


def get_total_cores_generic(client: Client) -> int:
    """
    Retrieve the total number of cores known to the Dask scheduler through the
    client connection.
    """
    return sum(client.ncores().values())


def get_total_cores_jobqueuecluster(cluster: JobQueueCluster) -> int:
    """
    Retrieve the total number of cores from a Dask cluster connected to some
    kind of batch system (HTCondor, Slurm...).
    """
    # Wrapping in a try-block in case any of the dictionaries do not have the
    # needed keys
    try:
        # In some cases the Dask scheduler doesn't know about available workers
        # at creation time. Most notably, when using batch systems like HTCondor
        # through dask-jobqueue, creating the cluster object doesn't actually
        # start the workers. The scheduler will know about available workers in
        # the cluster only after cluster.scale has been called and the resource
        # manager has granted the requested jobs. So at this point, we can only
        # rely on the information that was passed by the user as a specification
        # of the cluster object. This comes in the form:
        # {'WORKER-NAME-1': {'cls': <class 'dask.WORKERCLASS'>,
        #                    'options': {'CORES_OR_NTHREADS': N, ...}},
        #  'WORKER-NAME-2': {'cls': <class 'dask.WORKERCLASS'>,
        #                    'options': {'CORES_OR_NTHREADS': N, ...}}}
        # This concept can vary between different types of clusters, but in the
        # cluster types defined in dask-jobqueue the keys of the dictionary above
        # refer to the name of a job submission, which can then involve multiple
        # cores of a node.
        workers_spec: Dict[str, Any] = cluster.worker_spec
        # For each job, there is a sub-dictionary that contains the 'options'
        # key, which value is another dictionary with all the information
        # specified when creating the cluster object. This contains also the
        # 'cores' key for any type of dask-jobqueue cluster.
        return sum(spec["options"]["cores"] for spec in workers_spec.values())
    except KeyError as e:
        raise RuntimeError("Could not retrieve the provided worker specification from the Dask cluster object. "
                           "Please report this as a bug.") from e


def get_total_cores(client: Client) -> int:
    """
    Retrieve the total number of cores of the Dask cluster.
    """
    try:
        # It may happen that the user is connected to a batch system. We try
        # to import the 'dask_jobqueue' module lazily to avoid a dependency.
        from dask_jobqueue import JobQueueCluster
        if isinstance(client.cluster, JobQueueCluster):
            return get_total_cores_jobqueuecluster(client.cluster)
    except ModuleNotFoundError:
        # We are not using 'dask_jobqueue', fall through to generic case
        pass

    return get_total_cores_generic(client)


class DaskBackend(Base.BaseBackend):
    """Dask backend for distributed RDataFrame."""

    def __init__(self, daskclient: Optional[Client] = None):
        super(DaskBackend, self).__init__()
        # If the user didn't explicitly pass a Client instance, the argument
        # `daskclient` will be `None`. In this case, we create a default Dask
        # client connected to a cluster instance with N worker processes, where
        # N is the number of cores on the local machine.
        self.client = (daskclient if daskclient is not None else
                       Client(LocalCluster(n_workers=os.cpu_count(), threads_per_worker=1, processes=True)))
        
        
    def optimize_npartitions(self) -> int:
        """
        Attempts to compute a clever number of partitions for the current
        execution. Currently it is the number of cores of the Dask cluster,
        either retrieved if known or inferred from the user-provided cluster
        specification.
        """
        return get_total_cores(self.client)


    def ProcessAndMerge(self, ranges, mapper, reducer, live_visualization_enabled, histogram_ids, local_nodes):
        """
        Performs map-reduce using Dask framework.

        Args:
            mapper (function): A function that runs the computational graph
                and returns a list of values.

            reducer (function): A function that merges two lists that were
                returned by the mapper.

        Returns:
            list: A list representing the values of action nodes returned
            after computation (Map-Reduce).
        """

        # These need to be passed as variables, because passing `self` inside
        # following `dask_mapper` function would trigger serialization errors
        # like the following:
        #
        # AttributeError: Can't pickle local object 'DaskBackend.ProcessAndMerge.<locals>.dask_mapper'
        # TypeError: cannot pickle '_asyncio.Task' object
        #
        # Which boil down to the self.client object not being serializable
        #         
        headers = self.headers
        shared_libraries = self.shared_libraries

        def dask_mapper(current_range):
            """
            Gets the paths to the file(s) in the current executor, then
            declares the headers found.

            Args:
                current_range (tuple): The current range of the dataset being
                    processed on the executor.

            Returns:
                function: The map function to be executed on each executor,
                complete with all headers needed for the analysis.
            """
            # Retrieve the current worker local directory
            localdir = get_worker().local_directory

            # Get and declare headers on each worker
            headers_on_executor = [
                os.path.join(localdir, os.path.basename(filepath))
                for filepath in headers
            ]
            Utils.declare_headers(headers_on_executor)

            # Get and declare shared libraries on each worker
            shared_libs_on_ex = [
                os.path.join(localdir, os.path.basename(filepath))
                for filepath in shared_libraries
            ]
            Utils.declare_shared_libraries(shared_libs_on_ex)

            ret = mapper(current_range)

            return ret

        dmapper = dask.delayed(dask_mapper)
        dreducer = dask.delayed(reducer)

        mergeables_lists = [dmapper(range) for range in ranges]

        if live_visualization_enabled:
            print("Live visualization enabled")

            #test_delayed_tasks = [dmapper(range) for range in ranges]
            test_delayed_tasks = mergeables_lists

            # Convert the list of delayed objects into a list of futures
            test_future_tasks = self.client.compute(test_delayed_tasks)
        
            # Create a new canvas
            backend_pad = ROOT.TVirtualPad.TContext()

            num_histograms = len(histogram_ids)
            canvas_rows = math.ceil(math.sqrt(num_histograms))
            canvas_cols = math.ceil(num_histograms / canvas_rows)
            canvas_width = 600 * canvas_cols
            canvas_height = 300 * canvas_rows
            c = ROOT.TCanvas("distrdf_backend", "distrdf_backend", canvas_width, canvas_height)
            canvas_divided = False

            # Create a dictionary to store cumulative histograms for each division
            cumulative_histograms = {}
            cumulative_sum = 0

            for future in as_completed(test_future_tasks):
                mergeables = future.result().mergeables
                if not canvas_divided:
                    # Divide the canvas into pads based on the number of histograms if not already done
                    c.Divide(canvas_rows, canvas_cols)
                    canvas_divided = True

                pad_num = 1
                for i, mergeable in enumerate(mergeables):
                    operation = local_nodes[i].operation.name
                    if operation == "Histo1D" and local_nodes[i].node_id in histogram_ids:
                        # Return the actual histogram object
                        h = mergeable.GetValue()  

                        if i not in cumulative_histograms:
                            cumulative_histograms[i] = h.Clone()
                        else:
                            cumulative_histograms[i].Add(h)

                        cumulative_histograms[i].SetFillColor(random.randint(1, 9))

                        # Move to the appropriate pad
                        pad = c.cd(pad_num)
                        cumulative_histograms[i].Draw()
                        
                        # Update the pad
                        pad.Update()
                        pad_num += 1
                    
                    # Treatment of other operations
                    '''
                    elif operation == "Sum":
                        cumulative_sum += mergeable.GetValue() 
                        print("Current Sum = ", cumulative_sum)
                    elif operation == "Mean":
                        print("Current Mean = ", mergeable.GetValue())
                    elif operation == "Max":
                        print("Current Max = ", mergeable.GetValue())
                    '''

            time.sleep(3)
            c.Close()
        else:
            print("Live visualization disabled")
        

        while len(mergeables_lists) > 1:
            mergeables_lists.append(
                dreducer(mergeables_lists.pop(0), mergeables_lists.pop(0)))

        # Here we start the progressbar for the current RDF computation graph
        # running on the Dask client. This expects a future object, so we need
        # convert the last delayed object from the list above to a future
        # through the `persist` call. This also starts the computation in the
        # background, but the time difference is negligible. The progressbar is
        # properly shown in the terminal, whereas in the notebook it can be
        # shown only if it's the last call in a cell. Since we're encapsulating
        # it in this class, it won't be shown. Full details at
        # https://docs.dask.org/en/latest/diagnostics-distributed.html#dask.distributed.progress
        final_results = mergeables_lists.pop().persist()
        progress(final_results)
        
        ret = final_results.compute()

        return ret

    def distribute_unique_paths(self, paths):
        """
        Dask supports sending files to the workers via the `Client.upload_file`
        method. Its stated purpose is to send local Python packages to the
        nodes, but in practice it uploads the file to the path stored in the
        `local_directory` attribute of each worker.
        """
        for filepath in paths:
            self.client.upload_file(filepath, load=False)

    def make_dataframe(self, *args, **kwargs):
        """
        Creates an instance of distributed RDataFrame that can send computations
        to a Dask cluster.
        """
        # Set the number of partitions for this dataframe, one of the following:
        # 1. User-supplied `npartitions` optional argument
        npartitions = kwargs.pop("npartitions", None)
        headnode = HeadNode.get_headnode(self, npartitions, *args)
        return DataFrame.RDataFrame(headnode)
