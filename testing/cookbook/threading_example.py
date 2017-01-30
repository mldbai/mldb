#
# threading_examplepy
# Mich, 2016-04-11
# Copyright (c) 2016 mldb.ai inc. All rights reserved.
#
# Threads can be usefull in python whenever you need to push multiple taks to
# mldb and would like to keep a synchronous handle over them. An example use
# case would be downloading and applying transform operations over multiple
# files.
#
# NOTE: You need to have threadpool in your python environment. If you are
# confident about your operations, it is possible to achieve the same result
# using multiprocessing.pool.ThreadPool. In python 2 the implementation lacks
# the error_callback attribute to allow error handling. Python 3 has it.

from threadpool import ThreadPool, makeRequests
import threading
import time
import traceback

mldb = mldb_wrapper.wrap(mldb)  # noqa

def some_func(food, **kwargs):
    time.sleep(0.1)
    mldb.log(threading.currentThread().name + " -> " + food)


def run_example():
    num_workers = 3
    pool = ThreadPool(num_workers)

    # This variable will tell us whether all threads worked or not. Stored in
    # an object (list) otherwise the inner definition cannot modify it.
    success = [True]

    # The exception handler is not required, but if it's not used the error
    # will be silent.
    def exc_handler(work_request, exc_info):
        mldb.log(traceback.format_tb(exc_info[2]))
        exception_type = exc_info[0]
        exception_message = exc_info[1]
        mldb.log(str(exception_type) + ': ' + str(exception_message))
        success[0] = False

        # If there is an error, stop all threads as soon as possible
        pool.dismissWorkers(num_workers, do_join=True)

    # makeRequests takes, as a second argument, a list of tuples where the
    # first element is *args and the second one is **kwargs and where each
    # tuple represents a job to run.
    #
    # Here we schedule two jobs.
    requests = makeRequests(some_func, [(['patate'], {}), (['orange'], {})],
                            exc_callback=exc_handler)
    [pool.putRequest(req) for req in requests]

    # pool.wait will raise an exception if an error occurs in an early jobs and
    # more jobs need to be run. It's ok, if there is an error we want to stop
    # anyway.
    pool.wait()

    # If the error occurred in one of the last jobs pool.wait will have worked
    # so we need to check it anyway.
    if not success[0]:
        mldb.log("An error occured")
        return

    # It is important (MLDBFB-470) to properly dismiss the workers
    pool.dismissWorkers(num_workers, do_join=True)
    mldb.log("Out of main thread")


if __name__ == '__main__':
    run_example()
    mldb.script.set_return("success")
