#
# mldb_logging_stream_handler.py
# Mich, 2016-04-11
# This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.
#
# How to build a logging StreamHandler that will output to MLDB since the
# regular stream handler won't work.
#
# Tow things can be done.
# 1 - Redirect sys.stdout and sys.stderr to mldb.log.
# 2 - Create a customer handler.
# This demoes the later.

import logging

mldb = mldb_wrapper.wrap(mldb)  # noqa

class MldbStreamHandler(logging.StreamHandler):
    def emit(self, record):
        mldb.log(self.format(record))


if __name__ == '__main__':
    log = logging.getLogger(__name__)
    log.addHandler(MldbStreamHandler())
    log.setLevel(logging.INFO)
    log.info("Info")
    log.error("Error")
    log.debug("Debug")
    mldb.script.set_return("success")
