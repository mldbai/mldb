#
# threading_examplepy
# Mich, 2016-04-11
# Copyright (c) 2016 Datacratic Inc. All rights reserved.
#
# Threads can be usefull in python whenever you need to push multiple taks to
# mldb and would like to keep a synchronous handle over them. An example use
# case would be downloading and applying transform operations over multiple
# files.
#
# NOTE: You need to have threadpool in your python environment.

from threadpool import ThreadPool, makeRequests
import threading
import time

mldb = mldb_wrapper.wrap(mldb)  # noqa

def some_func(food, **kwargs):
    time.sleep(0.1)
    mldb.log(threading.currentThread().name + " -> " + food)


def run_example():
    num_workers = 3
    pool = ThreadPool(num_workers)
    # makeRequests takes, as a second argument, a list of tuples where the
    # first element is *args and the second one is **kwargs and where each
    # tuple represents a job to run.
    #
    # Here we schedule two jobs.
    requests = makeRequests(some_func, [(['patate'], {}), (['orange'], {})])
    [pool.putRequest(req) for req in requests]
    pool.wait()

    # It is important (MLDBFB-470) do properly dismiss the workers
    pool.dismissWorkers(num_workers, do_join=True)
    mldb.log("Out of main thread")


if __name__ == '__main__':
    run_example()
    mldb.script.set_return("success")
