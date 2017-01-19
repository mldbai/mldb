#!/usr/bin/env python
# Copyright mldb.ai inc 2016
# Author: Jean Raby <jean@mldb.ai>

# This script is called by runsv when the mldb service exits.
# See http://smarden.org/runit/runsv.8.html for more details.
# Two arguments are given to ./finish:
# The first one is ./run's exit code, or -1 if ./run didn't exit normally.
# The second one is the least significant byte of the exit status as
# determined by waitpid(2); for instance it is 0 if ./run exited normally,
# and the signal number if ./run was terminated by a signal.
# If runsv cannot start ./run for some reason, the exit code is 111 and the status is 0.

import os
import signal
import sys

sigmap = { 4:  "SIGILL: illegal instruction (internal error)",
           6:  "SIGABRT: abort(3) called (internal error)",
           8:  "SIGFPE: divide-by-zero (internal error)",
           9:  "SIGKILL: killed from outside (external cause, maybe the OOM Killer)",
           11: "SIGSEGV: segmentation fault (internal error)",
           15: "SIGTERM: regular shutdown",
         }

msg = ""
sig = None
if len(sys.argv) == 3:
    exit_code = sys.argv[1]
    status_code = sys.argv[2]
    if os.WIFSIGNALED(int(status_code)):
        sig = os.WTERMSIG(int(status_code))

print
if sig == None:
    if exit_code == "0":
        print "MLDB exited, shutting down container."
        os.kill(1, signal.SIGTERM)  # Tell init to terminate every process.
    else:
        print "MLDB exited abnormally, aborting container."
        # Aborting the container properly is tricky.
        # The following tries to kill process in proper order so that my_init
        # will exit with a non-zero exit code.

        # 1. Kill runsvdir with SIGHUP so it exits with 111.
        #    my_init will see this and start binging down all services and
        #    eventually exit with exit code 111.
        os.system("/usr/bin/pkill -HUP runsvdir")

        # 2. Kill our parent runsv so it doesn't manage the service anymore
        #    runsvdir should be dead/dying now, so it won't try to restart it.
        os.kill(os.getppid(), signal.SIGKILL)

        # 3. We can go now.
        exit(1)
else:
    msg = "MLDB exited due to signal %d" % (sig)
    if sig in sigmap:
        msg += " " + sigmap[sig]
    print msg
print


