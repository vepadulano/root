from concurrent.futures import ThreadPoolExecutor


class Reducer:

    @staticmethod
    def tree_reduce(reducer, iterable, chunk_size=2, min_size=4):
        """
        Parallel tree reduction.
        At each step objects to reduce are divided into
        groups (chunks) and each chunk is then reduced to one
        object in parallel. Whole process lasts until there is
        at most min_size objects, which are then reduced
        sequentially.
        """

        to_process = iterable

        while len(to_process) > min_size:
            chunks = Reducer.divide_into_chunks(to_process, chunk_size=chunk_size)
            to_process = Reducer.parallel_reduce(chunks, reducer)

        return Reducer.reduce(reducer, to_process)

    @staticmethod
    def divide_into_chunks(iterable, chunk_size=2):
        """
        Divide list into chunks of given size.
        If even division is impossible, leftovers are put in the last entry.

        Returns:
            list: List of tuples each of size chunk_size.

        >>> r = Reducer()
        >>> r.divide_into_chunks([1, 2, 3, 4, 5])
        [(1, 2), (3, 4), (5,)]
        >>> r.divide_into_chunks([1, 2, 3, 4, 5, 6], chunk_size=3)
        [(1, 2, 3), (4, 5, 6)]
        >>> r.divide_into_chunks([], chunk_size=1)
        []

        """

        if chunk_size <= 0:
            return []

        return [tuple(iterable[i:i+chunk_size])
                for i in range(0, len(iterable), chunk_size)]

    @staticmethod
    def parallel_reduce(chunks, reducer):
        with ThreadPoolExecutor(len(chunks)) as executor:
            futures = [executor.submit(Reducer.reduce, reducer, chunk)
                       for chunk in chunks]
            results = [future.result() for future in futures]
        return results
    
    @staticmethod
    def reduce(reducer, iterable):
        if not iterable:
            return None

        acc = iterable[0]
        for i in range(1, len(iterable)):
            acc = reducer(acc, iterable[i])

        return acc
