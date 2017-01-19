#!/usr/bin/env python
# Copyright mldb.ai inc 2016
# Author: Jean Raby <jean@mldb.ai>

# TODO:
#  - configure logging so that access/error logs go somewhere else than stderr

import fcntl
import functools
import grp
import jinja2
import os
import pwd
import pytz
import sys
import time

import tornado.web

from tornado.ioloop import IOLoop
from datetime import datetime
from collections import namedtuple, deque

try:
  from mldb_logger_utils import RUNAS, HTTP_LISTEN_PORT
except NameError:
  # provide defaults if templating didn't run
  RUNAS = "nobody"
  HTTP_LISTEN_PORT = 1234

LOGBUFSIZE = 8192


LogLine = namedtuple('LogLine', ['dt', 'data', ])
LOGS_MLDB_TEMPLATE = \
"""
<html><body>
<pre>
{%- for l in logbuf -%}
{{l.dt.strftime(timeformat)}}    {{l.data}}
{%- endfor %}
</pre>
<a name="end"></a>
</body></html>
"""

def droppriv():
  if os.getuid() != 0:
      return  # not root?

  new_uid = pwd.getpwnam(RUNAS).pw_uid
  new_gid = grp.getgrnam(RUNAS).gr_gid

  os.setgroups([])
  os.setgid(new_gid)
  os.setuid(new_uid)
  old_umask = os.umask(077)

def stdin_ready(f, logbuf, fd, events):
  if events & IOLoop.READ:
    try:
      for line in f:
        logline = LogLine(dt=datetime.now(pytz.utc), data=line.decode('utf8', 'replace'))
        logbuf.append(logline)
        sys.stdout.write(line)
        # line buffering is needed to make sure message are emitted in realtime
        # simulate that by flushing every line...
        sys.stdout.flush()
    except IOError:
      pass  # If we get a EWOULDBLOCK, continue. EOF handled below

  if events & IOLoop.ERROR:
      exit(0)

class LogsMldbHandler(tornado.web.RequestHandler):
  def get(self):
    """ Sends the last n lines from logbuf, or all of it if n is not set """
    n = self.get_argument("n", default=None)
    try:
      timeformat = "%FT%T.%f%z"
      if logbuf[0].dt.tzname() == "UTC":
        timeformat = "%FT%T.%fZ"
    except IndexError:
      pass  # don't care, logbuf is probably empty

    env = { "timeformat": timeformat,
            "logbuf": list(logbuf)[-int(n):] if n else logbuf
          }
    out = jinja2.Environment().from_string(LOGS_MLDB_TEMPLATE).render(**env)
    self.set_header('Content-Type', 'text/html')
    self.write(out)


if __name__ == "__main__":

  droppriv()  # Early on, we don't need privileges for anything.

  logbuf = deque(maxlen=LOGBUFSIZE)
  io_loop = IOLoop.current()

  # set stdin to non blocking mode for use with tornado
  fl = fcntl.fcntl(sys.stdin.fileno(), fcntl.F_GETFL)
  fcntl.fcntl(sys.stdin.fileno(), fcntl.F_SETFL, fl | os.O_NONBLOCK)

  callback = functools.partial(stdin_ready, sys.stdin, logbuf)
  io_loop.add_handler(sys.stdin.fileno(), callback,
                      io_loop.READ | io_loop.ERROR)

  app = tornado.web.Application([ ("/logs/mldb", LogsMldbHandler) ])
  app.listen(HTTP_LISTEN_PORT)

  io_loop.start()

