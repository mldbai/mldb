#
# MLDB-1869_json_payload_test.py
# Francois-Michel L Heureux, 2016-08-01
# This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.
#
import requests
import json

mldb = mldb_wrapper.wrap(mldb)  # noqa
url = 'http://localhost:' + mldb.get_http_bound_address().split(':')[-1]

class Mldb1869JsonPayloadTest(MldbUnitTest):  # noqa

    def test_base(self):
        "Shows that the connection works"
        r = requests.get(url + '/v1/datasets')
        self.assertEqual(r.status_code, 200, r.text)
        self.assertEqual(r.json(), [])

    def test_clean_put(self):
        r = requests.put(url + '/v1/datasets/ds1',
                         data='{"type" : "sparse.mutable"}')
        self.assertEqual(r.status_code, 201, r.text)

    def test_put_with_linux_new_line(self):
        r = requests.put(url + '/v1/datasets/ds_linux1',
                         data='{"type" : "sparse.mutable"}\n')
        self.assertEqual(r.status_code, 201, r.text)

        r = requests.put(url + '/v1/datasets/ds_linux2',
                         data='{\n"type" : "sparse.mutable"\n}\n')
        self.assertEqual(r.status_code, 201, r.text)

    def test_put_with_dos_new_line(self):
        r = requests.put(url + '/v1/datasets/ds_dos1',
                         data='{"type" : "sparse.mutable"}\r\n')
        r = requests.put(url + '/v1/datasets/ds_dos2',
                         data='{\r\n"type" : "sparse.mutable"\r\n}\r\n')
        self.assertEqual(r.status_code, 201, r.text)

if __name__ == '__main__':
    mldb.run_tests()
