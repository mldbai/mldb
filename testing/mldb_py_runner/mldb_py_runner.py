# This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

#
# runner.py
# Mich, 2015-03-03
# Copyright (c) 2015 mldb.ai inc.  All rights reserved.
#
# Launches mldb
#

import os
import json
import tempfile
import subprocess
import time
import logging
import signal
import prctl


def pre_exec():
    # new process group - all our child will be in that group
    prctl.set_pdeathsig(signal.SIGTERM)
    os.setpgid(0, 0)


class MldbRunner(object):
    """
    RIAA style object that launches mldb and keep it running as long as it's
    not destroyed.
    """

    def __init__(self):
        tmpdir = tempfile.mkdtemp()
        fifoname = os.path.join(tmpdir, 'myfifo')
        os.mkfifo(fifoname)
        env = os.environ.copy()
        env["LITERATE_DOC_BIND_FILENAME"] = fifoname
        if 'VIRTUALENV' in env:
            env['PATH'] = "%s/bin:%s" % (env['VIRTUALENV'], env.get('PATH', ''))
        self.mldb = None
        self.mldb = subprocess.Popen(["build/x86_64/bin/mldb_runner",
                                      "--http-listen-port", "8000-12000"],
                                     env=env,
                                     preexec_fn=pre_exec)
        try:
            fifo = open(fifoname, 'r')

            # functions until api is ready
            self.port = json.loads(fifo.read())["port"]

            fifo.close()
            os.remove(fifoname)
            os.rmdir(tmpdir)

        except:
            log = logging.getLogger(__name__)
            log.exception("")
            self.mldb.terminate()
            self.mldb.wait()
            self.mldb = None

    def __del__(self):
        if self.mldb:
            self.mldb.terminate()
            self.mldb.wait()
            try:
                os.killpg(self.mldb.pid, signal.SIGTERM)
            except OSError:
                # likely no child to kill
                pass

    def loop_while_alive(self):
        while self.mldb.poll() is None:
            try:
                time.sleep(1)
            except KeyboardInterrupt:
                break
