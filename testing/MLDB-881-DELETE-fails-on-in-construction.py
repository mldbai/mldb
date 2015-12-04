# This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

import json
import sys
import time


def parse_response(res):
    return json.loads(res['response'])

def check_res(res, value):
    if res['statusCode'] != value:
        mldb.log(res)
        assert False, "response status code is %s but expected code %s" % (res["statusCode"], value)

def test_delete_on_construction():
    # create an expensive resource async
    resp = mldb.perform("PUT", "/v1/datasets/dummy2", [], {
        'type' : 'text.line',
        'params' : {
            'dataFileUrl': 'http://files.figshare.com/1310438/reddit_user_posting_behavior.csv.gz'
        }
    },  [['async','true']])
    # resource should be under construction
    check_res(resp, 201)
    assert parse_response(resp)['state'] == 'initializing', 'the resource should still be under construction'
    
    # deleting that resource will wait until it is constructed
    resp = mldb.perform("DELETE", "/v1/datasets/dummy2", [], {}, [['async','true']])
    check_res(resp, 204) 
        
    # once the DELETE returns the resource should have been deleted
    resp = mldb.perform("GET", "/v1/datasets/dummy2", [], {});
    check_res(resp, 404)

test_delete_on_construction()

mldb.script.set_return('success')
