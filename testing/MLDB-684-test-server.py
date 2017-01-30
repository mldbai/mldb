# This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

# MLDB-684 test case: server

import json
from bottle import route, abort, default_app

@route('/ping', method='GET')
def ping():
    return "1"

@route('/doc/doc.html', method='GET')
def getDoc():
    return """
Hello, here is some documentation
"""

@route('/status', method='GET')
def getStatus():
    return json.dumps({"a-ok": True});

@route('/routes/goodbye', method='GET')
def getDoc():
    return "goodbye";

app = default_app()


### BEGIN BOILERPLATE

import sys
from wsgiref.simple_server import make_server, demo_app

port = 0

for port in range(22000,23000):
    try:
        httpd = make_server('localhost', port, app)
        # Make sure this gets out straight away
        sys.stdout.flush()
        break;
    except:
        pass

print "MLDB PLUGIN http http://localhost:%d" % port
sys.stdout.flush()




# Respond to requests until process is killed
httpd.serve_forever()

