#!/usr/bin/env python
# Copyright Datacratic 2016
# Author: Jean Raby <jean@datacratic.com>

# Wrapper around tee -a
# Read from stdin and write to logfile and to stdout

import os
tee = "/usr/bin/tee"
os.execv(tee,[tee, '-a', '{{MLDB_LOGFILE}}'])

