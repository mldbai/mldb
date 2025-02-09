#
# to_junit.py
# Nicolas Kructhen, 2013-03-26
# Mich, 2015-03-05
# Copyright (c) 2013 mldb.ai inc.  All rights reserved.
#
# make test | python junit.py > testresults.xml

import os
import fileinput
import re
from xml.etree import ElementTree
from io import BytesIO
from sys import stderr
import time
from threading import Timer

# thanks to
# https://stackoverflow.com/questions/14693701/how-can-i-remove-the-ansi-escape-sequences-from-a-string-in-python
ansi_escape = re.compile(r'\x1b[^m]*m')


start_time = time.time()

started = set()
passed = set()
passed_times = dict()
failed = set()

timer = None

def print_long_running():
    global timer
    running = started - passed - failed
    if len(running) > 0:
        stderr.write("\nRunning tests: " + str(running) + "\n")
    timer = Timer(10, print_long_running)
    timer.start()

print_long_running()

passed_pat = re.compile("\[\s*(.*)s\s*(.*)G\s*(.*)c\s*\]\s*(.*?) passed")
failed_pat = re.compile("(.*?) FAILED")
started_pat = re.compile("(\[MLDBTEST\]|\[TESTCASE\])\s*(.*?)$")
restart_pat = re.compile("TEST SUITE RESTART")

for l in fileinput.input():
    stderr.write(l)
    clean_line = l \
        .replace("\x1b[0m", "") \
        .replace("\x1b[32m", "") \
        .replace("\x1b[35m", "") \
        .replace("\x1b[31m", "") \
        .replace("\x1b[1;30m", "")\
        .replace("\x1b[0;39;49m", "")\
        .strip()

    if restart_pat.match(clean_line):
        started = set()

    if started_pat.match(clean_line):
#        print("clean", clean_line)
        test = started_pat.search(clean_line).groups()[1]
#        print("started", test)
        started.add(test)
    if passed_pat.match(clean_line):
        groups = passed_pat.search(clean_line).groups()
        cpu_time, memory_gigs, num_cores, test = groups
        approx_wall_time = float(cpu_time)/max(0.05, float(num_cores))
        passed.add(test)
        try:
            started.remove(test)
        except:
            pass
        passed_times[test] = str(approx_wall_time)
    if failed_pat.match(clean_line):
        test = failed_pat.search(clean_line).groups()[0]
        try:
            started.remove(test)
        except:
            pass
        failed.add(test)

failed.difference_update(passed)

builder = ElementTree.TreeBuilder()
builder.start('testsuite', {
    'errors'   : '0',
    'tests'    : str(len(passed) + len(failed)),
    'time'     : str(time.time() - start_time),
    'failures' : str(len(failed)),
    'name'     : 'tests'
})

for f in failed:
    fail_content = ""

    if os.path.isfile("build/x86_64/tests/%s.failed" % f):
        with open("build/x86_64/tests/%s.failed" % f, "r") as failFile:
            fail_content = failFile.read().replace(chr(27), "")
    builder.start('testcase', {
        'time' : '0',
        'name' : f
    })
    builder.start('failure', {
        'type' : 'failure',
        'message' : 'Check log'
    })
    ansi_escape.sub('', fail_content)
    builder.data(fail_content)
    builder.end('failure')
    builder.end('testcase')

for p in passed:
    builder.start('testcase', {
        'time' : passed_times[p],
        'name' : p
    })
    builder.end('testcase')

builder.end('testsuite')

tree = ElementTree.ElementTree()
element = builder.close()
tree._setroot(element)
io = BytesIO()
tree.write(io, encoding='utf-8', xml_declaration=True)
print(io.getvalue().decode('utf-8'))

if timer is not None:
    timer.cancel()
