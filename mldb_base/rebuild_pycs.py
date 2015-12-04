#!/usr/bin/python
# Copyright (c) 2015 Datacratic Inc.  All rights reserved.

import os
import py_compile

done_file = '/usr/local/lib/python2.7/dist-packages/.rebuild_pycs_done'
if os.path.isfile(done_file):
  exit(0)

for (dirpath, dirnames, files) in os.walk('/usr/local/lib/python2.7/dist-packages'):
    for file in files:
      if file.endswith('.py'):
        py_compile.compile(os.path.join(dirpath,file))

open(done_file, 'a')
