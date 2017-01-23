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
from StringIO import StringIO

# thanks to
# https://stackoverflow.com/questions/14693701/how-can-i-remove-the-ansi-escape-sequences-from-a-string-in-python
ansi_escape = re.compile(r'\x1b[^m]*m')


passed = set()
passed_times = dict()
failed = set()

passed_pat = re.compile("\[\s*(.*)s\s*(.*)G\s*(.*)c\s*\]\s*(.*?) passed")
failed_pat = re.compile("(.*?) FAILED")
for l in fileinput.input():
    clean_line = l \
        .replace("\x1b[0m", "") \
        .replace("\x1b[32m", "") \
        .replace("\x1b[35m", "") \
        .replace("\x1b[31m", "") \
        .replace("\x1b[1;30m", "")\
        .strip()
        
    if passed_pat.match(clean_line):
        groups = passed_pat.search(clean_line).groups()
        cpu_time, memory_gigs, num_cores, test = groups
        approx_wall_time = float(cpu_time)/max(0.05, float(num_cores))
        passed.add(test)
        passed_times[test] = str(approx_wall_time)
    if failed_pat.match(clean_line):
        test = failed_pat.search(clean_line).groups()[0]
        failed.add(test)

failed.difference_update(passed)

builder = ElementTree.TreeBuilder()
builder.start('testsuite', {
    'errors'   : '0',
    'tests'    : str(len(passed) + len(failed)),
    'time'     : '0',
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
    try:
        builder.data(unicode(fail_content, 'utf-8'))
    except:
        builder.data(unicode(fail_content, 'latin-1'))
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
io = StringIO()
tree.write(io, encoding='utf-8', xml_declaration=True)
print io.getvalue()
