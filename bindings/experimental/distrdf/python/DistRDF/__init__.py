#  @author Vincenzo Eduardo Padulano
#  @author Enric Tejedor
#  @date 2021-02

################################################################################
# Copyright (C) 1995-2022, Rene Brun and Fons Rademakers.                      #
# All rights reserved.                                                         #
#                                                                              #
# For the licensing terms see $ROOTSYS/LICENSE.                                #
# For the list of contributors see $ROOTSYS/README/CREDITS.                    #
################################################################################
from __future__ import annotations

import logging
import os
import types

import concurrent.futures

from typing import Iterable, TYPE_CHECKING

from DistRDF.Backends import build_backends_submodules
from DistRDF.LiveVisualize import LiveVisualize


if TYPE_CHECKING:
    from DistRDF.Proxy import ResultPtrProxy, ResultMapProxy

logger = logging.getLogger(__name__)


def initialize(fun, *args, **kwargs):
    """
    Set a function that will be executed as a first step on every backend before
    any other operation. This method also executes the function on the current
    user environment so changes are visible on the running session.

    This allows users to inject and execute custom code on the worker
    environment without being part of the RDataFrame computational graph.

    Args:
        fun (function): Function to be executed.

        *args (list): Variable length argument list used to execute the
            function.

        **kwargs (dict): Keyword arguments used to execute the function.
    """
    from DistRDF.Backends import Base
    Base.BaseBackend.register_initialization(fun, *args, **kwargs)

def DeclareCppCode(code_to_declare: str) -> None:
    """
    Declare the C++ code that has to be processed on each worker. 
    Args:
        codeToDeclare (str): cpp code to be declared on the workers
    """
    from DistRDF.Backends import Base
    Base.BaseBackend.register_declaration(code_to_declare)

    
def DistributeHeaders(paths_to_headers: Iterable[str], df = None):
    """
    This function allows users to directly load c++ custom headers 
    onto the workers. 

    Args:
        paths_to_headers (list): list of paths to headers to be distributed
        
        df: dataframe for which the libraries are distributed

    """    
    from DistRDF.Backends import Base, Utils
    
    if df is None:
        Base.BaseBackend.register_headers(paths_to_headers) 

    else:
        headers_to_distribute = set()
        
        if isinstance(paths_to_headers, str):
            headers_to_distribute = (Utils.get_paths_set_from_string(paths_to_headers))
        else: 
            for path_to_header in paths_to_headers:
                sanatized_path_to_header = Utils.get_paths_set_from_string(path_to_header)
                headers_to_distribute.update(sanatized_path_to_header)
        
        Utils.declare_headers(headers_to_distribute)
        df._headnode.backend.distribute_unique_paths(headers_to_distribute)
        Base.BaseBackend.headers.update(headers_to_distribute)


def DistributeFiles(paths_to_files: Iterable[str], df = None):
    """
    This function allows users to directly load arbitrary files
    onto the workers. 

    Args:
        paths_to_files (list): list of paths to files to be distributed
        
        df: dataframe for which the libraries are distributed

    """
    from DistRDF.Backends import Base, Utils
    
    if df is None:
        Base.BaseBackend.register_files(paths_to_files)
    
    else: 
        files_to_distribute = set()
        if isinstance(paths_to_files, str):
            files_to_distribute = (Utils.get_paths_set_from_string(paths_to_files))
        else: 
            for path_to_file in paths_to_files:
                sanatized_path_to_file = Utils.get_paths_set_from_string(path_to_file)
                files_to_distribute.update(sanatized_path_to_file)

        df._headnode.backend.distribute_unique_paths(files_to_distribute)    

    
def DistributeSharedLib(paths_to_shared_libraries: Iterable[str], df=None) -> None:
    """
    This function allows users to directly load pre-compiled shared libraries 
    onto the workers. 

    Args:
        paths_to_shared_libraries (list): list of paths to shared libraries to be distributed
        
        df: dataframe for which the libraries are distributed

    """
    from DistRDF.Backends import Base, Utils

    if df is None:
        Base.BaseBackend.register_shared_lib(paths_to_shared_libraries)
    
    else: 
        libraries_to_distribute = set()
        pcms_to_distribute = set()
        
        if isinstance(paths_to_shared_libraries, str):
            pcms_to_distribute, libraries_to_distribute = (Utils.check_pcm_in_library_path(paths_to_shared_libraries))

        else:
            for path_string in paths_to_shared_libraries:
                pcm, libraries = Utils.check_pcm_in_library_path(
                    path_string
                )
                
                libraries_to_distribute.update(libraries)
                pcms_to_distribute.update(pcm)  
        
        Utils.declare_shared_libraries(libraries_to_distribute)
        df._headnode.backend.distribute_unique_paths(pcms_to_distribute)
        df._headnode.backend.distribute_unique_paths(libraries_to_distribute)
        
        Base.BaseBackend.shared_libraries.update(libraries_to_distribute)
        Base.BaseBackend.pcms.update(pcms_to_distribute)

def RunGraphs(proxies: Iterable) -> int:
    """
    Trigger the execution of multiple RDataFrame computation graphs on a certain
    distributed backend. If the backend doesn't support multiple job
    submissions concurrently, the distributed computation graphs will be
    executed sequentially.

    Args:
        proxies(list): List of action proxies that should be triggered. Only
            actions belonging to different RDataFrame graphs will be
            triggered to avoid useless calls.

    Return:
        (int): The number of unique computation graphs executed by this call.


    Example:

        @code{.py}
        import ROOT
        RDataFrame = ROOT.RDF.Experimental.Distributed.Dask.RDataFrame
        RunGraphs = ROOT.RDF.Experimental.Distributed.RunGraphs

        # Create 3 different dataframes and book an histogram on each one
        histoproxies = [
            RDataFrame(100)
                .Define("x", "rdfentry_")
                .Histo1D(("name", "title", 10, 0, 100), "x")
            for _ in range(4)
        ]

        # Execute the 3 computation graphs
        n_graphs_run = RunGraphs(histoproxies)
        # Retrieve all the histograms in one go
        histos = [histoproxy.GetValue() for histoproxy in histoproxies]
        @endcode


    """
    # Import here to avoid circular dependencies in main module
    from DistRDF.Proxy import execute_graph
    if not proxies:
        logger.warning("RunGraphs: Got an empty list of handles, now quitting.")
        return 0

    # Get proxies belonging to distinct computation graphs
    uniqueproxies = list({proxy.proxied_node.get_head(): proxy for proxy in proxies}.values())

    # Submit all computation graphs concurrently from multiple Python threads.
    # The submission is not computationally intensive
    with concurrent.futures.ThreadPoolExecutor(max_workers=len(uniqueproxies)) as executor:
        futures = [executor.submit(execute_graph, proxy.proxied_node) for proxy in uniqueproxies]
        concurrent.futures.wait(futures)

    return len(uniqueproxies)


def VariationsFor(actionproxy: ResultPtrProxy) -> ResultMapProxy:
    """
    Equivalent of ROOT.RDF.Experimental.VariationsFor in distributed mode.
    """
    # similar to resPtr.fActionPtr->MakeVariedAction()
    return actionproxy.create_variations()


def create_distributed_module(parentmodule):
    """
    Helper function to create the ROOT.RDF.Experimental.Distributed module.

    Users will see this module as the entry point of functions to create and
    run an RDataFrame computation distributedly.
    """
    distributed = types.ModuleType("ROOT.RDF.Experimental.Distributed")

    # PEP302 attributes
    distributed.__file__ = "<module ROOT.RDF.Experimental>"
    # distributed.__name__ is the constructor argument
    distributed.__path__ = []  # this makes it a package
    # distributed.__loader__ is not defined
    distributed.__package__ = parentmodule

    distributed = build_backends_submodules(distributed)

    # Inject top-level functions
    distributed.initialize = initialize
    distributed.RunGraphs = RunGraphs
    distributed.VariationsFor = VariationsFor
    distributed.LiveVisualize = LiveVisualize
    distributed.DistributeHeaders = DistributeHeaders
    distributed.DistributeFiles = DistributeFiles
    distributed.DistributeSharedLib = DistributeSharedLib
    distributed.DeclareCppCode = DeclareCppCode
    
    return distributed
