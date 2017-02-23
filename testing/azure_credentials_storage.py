#
# azure_credentials_storage.py
# Francois-Michel L'Heureux, 2017-02-23
# This file is part of MLDB. Copyright 2017 mldb.ai inc. All rights reserved.
#

import unittest
from os.path import isfile, join, expanduser
mldb = mldb_wrapper.wrap(mldb)  # noqa

class AzureCredentialsStorage(MldbUnitTest):  # noqa

    def test_it(self):
        mldb.put('/v1/credentials/azureCreds', {
            'store' : {
                'resource' : 'azureblob://foobar',
                'resourceType' : 'azure:blob',
                'credential' : {
                    'protocol' : 'https',
                    'location' : 'blob.core.windows.net',
                    'id' : 'monBeauId',
                    'secret' : 'monBeauSecret',
                    'validUntil' : '2030-01-01T00:00:00Z'
                }
            }
        })

    @unittest.skipIf(
        not isfile(join(expanduser('~'), '.azure_cloud_credentials')),
        "skipping because no credentials were found")
    def test_selection_of_creds_2(self):
        with open(join(expanduser('~'), '.azure_cloud_credentials')) as f:
            creds = f.readlines()
            for idx, cred in enumerate(creds):
                parts = cred.split(';')
                id_ = None
                key = None
                for part in parts:
                    if part.startswith('AccountName='):
                        id_ = part[len('AccountName='):]
                    elif part.startswith('AccountKey='):
                        key = part[len('AccountKey='):]
                if id_ is None or key is None:
                    raise Exception('Invalid line in azure credentials: {}'
                                    .format(idx + 1))

            mldb.post('/v1/credentials', {
                'store' : {
                    'resource' : 'azureblob://publicelementai',
                    'resourceType' : 'azure:blob',
                    'credential' : {
                        'protocol' : 'https',
                        'location' : 'blob.core.windows.net',
                        'id' : id_,
                        'secret' : key,
                        'validUntil' : '2030-01-01T00:00:00Z'
                    }
                }
            })

            res = mldb.query(
                "SELECT fetcher("
                "'azureblob://publicelementai/private/a_propos.txt'"
                ")")
            self.assertTrue(res[1][2] is None)
            self.assertGreater(len(res[1][1]["blob"]), 0)


if __name__ == '__main__':
    mldb.run_tests()
