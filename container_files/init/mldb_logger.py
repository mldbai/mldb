#!/usr/bin/env python
# Copyright Datacratic 2016
# Author: Jean Raby <jean@datacratic.com>

# TODO:
#  - configure logging so that access/error logs go somewhere else than stderr
#  - see if we can force line buffering on stdout in a more beautiful way
#  - add proper content-type

import functools
import os
import fcntl
import sys
import time
import pwd
import grp

import tornado.web

from tornado.ioloop import IOLoop
from datetime import datetime
from collections import namedtuple, deque

RINGBUFSIZE = 1024
HTTP_LISTEN_PORT = {{MLDB_LOGGER_HTTP_PORT}}  # From template_vars.mk
RUNAS = "_mldb"

LogLine = namedtuple('LogLine', ['dt', 'data', ])

logline_cnt = 0

def droppriv():
    if os.getuid() != 0:
        return  # not root?

    new_uid = pwd.getpwnam(RUNAS).pw_uid
    new_gid = grp.getgrnam(RUNAS).gr_gid

    os.setgroups([])
    os.setgid(new_gid)
    os.setuid(new_uid)
    old_umask = os.umask(077)

def stdin_ready(f, ringbuf, fd, events):
  global logline_cnt
  if events & IOLoop.READ:
    try:
      for line in f:
        logline = LogLine(dt=datetime.utcnow(), data=line)
        ringbuf.append(logline)
        logline_cnt += 1
        sys.stdout.write(line)
        # line buffering is needed to make sure message are emitted in realtime
        # simulate that by flushing every line...
        sys.stdout.flush()
    except IOError:
      # If we get a EWOULDBLOCK, continue
      # EOF handled below
      pass
  if events & IOLoop.ERROR:
      exit(0)

class LogsMldbHandler(tornado.web.RequestHandler):
  def get(self):
    self.write('<html><body><pre>\n')
    for l in log_lines_ringbuf:
      self.write("%s %s" % (l.dt.isoformat(), l.data))
    self.write('</pre><a name=end></body></html>\n')


if __name__ == "__main__":

  droppriv()  # Early on, we don't need privileges for anything.

  log_lines_ringbuf = deque(maxlen=RINGBUFSIZE)
  io_loop = IOLoop.current()

  # set stdin to non blocking mode for use with tornado
  fl = fcntl.fcntl(sys.stdin.fileno(), fcntl.F_GETFL)
  fcntl.fcntl(sys.stdin.fileno(), fcntl.F_SETFL, fl | os.O_NONBLOCK)

  callback = functools.partial(stdin_ready, sys.stdin, log_lines_ringbuf)
  io_loop.add_handler(sys.stdin.fileno(), callback,
                      io_loop.READ | io_loop.ERROR)

  app = tornado.web.Application([ ("/logs/mldb", LogsMldbHandler) ])
  app.listen(HTTP_LISTEN_PORT)

  try:
    t1 = time.time()
    io_loop.start()
  except KeyboardInterrupt:
    total_time = time.time() - t1
    sys.stderr.write("Got %d lines in %d sec: %f lines/s\n" % (logline_cnt, total_time, logline_cnt/total_time))
    raise
