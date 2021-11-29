from __future__ import print_function

import logging
import functools
import time

from DistRDF import DataFrame
from DistRDF import HeadNode
from DistRDF.Backends import Base

from .flushing_logger import FlushingLogger
from .AWS_utils import AWSServiceWrapper
from .reducer import Reducer
from concurrent.futures import ThreadPoolExecutor


class AWS(Base.BaseBackend):
    """
    Backend that executes the computational graph using using AWS Lambda
    for distributed execution.
    """

    MIN_NPARTITIONS = 8
    npartitions = 32
    TOKEN_PATH = '/tmp/certs'

    def __init__(self, config={}):
        """
        Config for AWS is same as in Dist backend,
        more support will be added in future.
        """
        super(AWS, self).__init__()
        self.logger = FlushingLogger() if logging.root.level >= logging.INFO else logging.getLogger()
        self.npartitions = self._get_partitions()
        self.region = config.get('region') or 'us-east-1'
        self.paths = []
        self.aws_service_wrapper = AWSServiceWrapper(self.region)

    def _get_partitions(self):
        return int(self.npartitions or AWS.MIN_NPARTITIONS)

    def make_dataframe(self, *args, **kwargs):
        """
        Creates an instance of distributed RDataFrame that can send computations
        to a Spark cluster.
        """
        # Set the number of partitions for this dataframe, one of the following:
        # 1. User-supplied `npartitions` optional argument
        # 2. An educated guess according to the backend, using the backend's
        #    `optimize_npartitions` function
        # 3. Set `npartitions` to 2
        npartitions = kwargs.pop("npartitions", self.optimize_npartitions())
        headnode = HeadNode.get_headnode(npartitions, *args)
        return DataFrame.RDataFrame(headnode, self)

    def ProcessAndMerge(self, ranges, mapper, reducer):
        """
        Performs map-reduce using AWS Lambda.
        Args:
            mapper (function): A function that runs the computational graph
                and returns a list of values.
            reducer (function): A function that merges two lists that were
                returned by the mapper.
        Returns:
            list: A list representing the values of action nodes returned
            after computation (Map-Reduce).
        """

        invoke_func, download_func, processing_bucket = self.create_init_arguments(mapper)

        self.logger.info(f'Before lambdas invoke. Number of lambdas: {len(ranges)}')

        invoke_begin = time.time()

        files = self.invoke_and_download(invoke_func, download_func, ranges)

        reduce_begin = time.time()

        result = Reducer.tree_reduce(reducer, files)

        bench = (
            len(ranges),
            reduce_begin - invoke_begin,
            time.time() - reduce_begin
        )

        # Clean up intermediate objects after we're done
        self.aws_service_wrapper.clean_s3_bucket(processing_bucket)

        print(bench)

        return result

    def create_init_arguments(self, mapper):
        """
        Create init arguments for ProcessAndMerge method.
        """

        pickled_mapper = AWSServiceWrapper.encode_object(mapper)
        pickled_headers = AWSServiceWrapper.encode_object(self.paths)
        processing_bucket = self.aws_service_wrapper.get_ssm_parameter_value('processing_bucket')

        print(self.TOKEN_PATH)
        try:
            f = open(self.TOKEN_PATH, "rb")
            certs = f.read()
        except FileNotFoundError:
            print("failed to find cert!")
            certs = b''

        invoke_lambda = functools.partial(
            self.aws_service_wrapper.invoke_root_lambda,
            script=pickled_mapper,
            certs=certs,
            headers=pickled_headers,
            bucket_name=processing_bucket,
            logger=self.logger)

        download_partial = functools.partial(
            self.aws_service_wrapper.get_partial_result_from_s3,
            bucket_name=processing_bucket)

        return invoke_lambda, download_partial, processing_bucket

    def invoke_and_download(self, invoke, download, ranges):
        files = []

        def download_callback(future):
            filename = future.result()
            if filename is not None:
                files.append(download(filename))

        with ThreadPoolExecutor(max_workers=len(ranges)) as executor:
            for root_range in ranges:
                future = executor.submit(invoke, root_range)
                future.add_done_callback(download_callback)

        if len(files) < len(ranges):
            raise Exception(f'Some lambdas failed after multiple retrials')

        return files

    def distribute_unique_paths(self, paths):
        """
        Spark supports sending files to the executors via the
        `SparkContext.addFile` method. This method receives in input the path
        to the file (relative to the path of the current python session). The
        file is initially added to the Spark driver and then sent to the
        workers when they are initialized.

        Args:
            paths (set): A set of paths to files that should be sent to the
                distributed workers.
        """
        pass

    def add_header(self, path: str):
        with open(path, 'r') as f:
            contents = f.read()
            self.paths.append((path, contents))
