# https://stackoverflow.com/questions/19663366/dlopen-fails-in-boost-python-module
# Otherwise all hell breaks loose
# Note that we should probably simply dlopen the mldb libraries with the correct flags
# using ctypes, and not change this (global) setting
import sys
import os
sys.setdlopenflags(os.RTLD_NOW | os.RTLD_GLOBAL)

from mldb.mldb_wrapper import mldb_wrapper, MldbUnitTest # noqa
from _mldb import find_mldb_environment # noqa

mldb = mldb_wrapper.wrap(find_mldb_environment())
MldbBaseException = mldb_wrapper.MldbBaseException
ResponseException = mldb_wrapper.ResponseException
TooManyRedirectsException = mldb_wrapper.TooManyRedirectsException
Response = mldb_wrapper.Response
StepsLogger = mldb_wrapper.StepsLogger
