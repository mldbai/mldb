#!/usr/bin/python
# Copyright (c) 2015 mldb.ai inc.  All rights reserved.

import os
import py_compile

# Skip dir and files containing py3
BLACKLIST_DIR = ['/usr/local/lib/python2.7/dist-packages/zmq/tests']
BLACKLIST_FILES = ['/usr/local/lib/python2.7/dist-packages/zmq/auth/asyncio.py',
                   '/usr/local/lib/python2.7/dist-packages/zmq/asyncio.py',
                   '/usr/local/lib/python2.7/dist-packages/pexpect/async.py',
                   '/usr/local/lib/python2.7/dist-packages/jinja2/asyncfilters.py',
                   '/usr/local/lib/python2.7/dist-packages/jinja2/asyncsupport.py',
                  ]
done_file = '/usr/local/lib/python2.7/dist-packages/.rebuild_pycs_done'
if os.path.isfile(done_file):
  exit(0)

for (dirpath, dirnames, files) in os.walk('/usr/local/lib/python2.7/dist-packages'):
    if dirpath in BLACKLIST_DIR:
      continue
    for file in files:
      if os.path.join(dirpath,file) in BLACKLIST_FILES:
        continue
      if file.endswith('.py'):
        py_compile.compile(os.path.join(dirpath,file))

open(done_file, 'a')
