# This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

import json
import sys
import time


def parse_response(res):
    return json.loads(res['response'])

def check_res(res, value):
    if res['statusCode'] != value:
        mldb.log(res)
        assert False, "response status code is %s but expected code %s" % (res["statusCode"], value)

def test_overwrite():
    # create an expensive resource async
    resp = mldb.perform("PUT", "/v1/datasets/dummy2", [], {
        'type' : 'text.line',
        'params' : {
            'dataFileUrl': 'https://public.mldb.ai/reddit.csv.gz'
        }
    },  [['async','true']])
    # resource should be under construction
    check_res(resp, 201)
    assert parse_response(resp)['state'] == 'initializing', 'the resource should still be under construction'
    
    # creating another resource at the same URI must fail
    resp = mldb.perform("PUT", "/v1/datasets/dummy2", [], {
        'type' : 'text.line',
        'params' : {
            'dataFileUrl': 'https://www.kaggle.com/c/facial-keypoints-detection/data?SampleSubmission.csv'
        }
    })
    check_res(resp, 409)
    assert 'being constructed' in parse_response(resp)['error'], 'the resource should still be under construction'
    
    # track the progress of the construction
    under_construction = True
    while(under_construction):
        time.sleep(0.2)
        resp = mldb.perform("GET", "/v1/datasets/dummy2", [], {});
        check_res(resp, 200)
        if parse_response(resp)['state'] == 'ok':
            under_construction = False
        
    # the construction is completed - should be able to overwrite the object
    resp = mldb.perform("PUT", "/v1/datasets/dummy2", [], {
        'type' : 'text.line',
        'params' : {
            'dataFileUrl': 'https://www.kaggle.com/c/facial-keypoints-detection/data?SampleSubmission.csv'
        }
    })
    check_res(resp, 201)
    assert 'kaggle.com' in parse_response(resp)['config']['params']['dataFileUrl'], 'the resource was not replaced'

    # last confirmation
    resp = mldb.perform("GET", "/v1/datasets/dummy2", [], {});
    check_res(resp, 200)
    assert 'kaggle.com' in parse_response(resp)['config']['params']['dataFileUrl'], 'the resource was not replaced'

test_overwrite()

mldb.script.set_return('success')
