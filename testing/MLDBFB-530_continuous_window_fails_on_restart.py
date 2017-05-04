#
# MLDBFB-530_continuous_window_fails_on_restart.py
# Mich, 2016-05-17
# This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.
#
from dateutil.tz import tzlocal
import datetime
import os
import unittest

mldb = mldb_wrapper.wrap(mldb)  # noqa

class ContinuousWindowDsIssueTest(MldbUnitTest):  # noqa

    def test_it(self):
        create_storage_js = """
        var config = { type: "beh.binary.mutable" };
        var dataset = mldb.createDataset(config);
        var output = { config: dataset.config() };
        output;
        """

        save_storage_js = """
        var uri = "file://tmp/MLDB-530-" + new Date().toISOString() + ".beh";
        var addr = "/v1/datasets/" + args.datasetId;
        var res = mldb.post(addr + "/routes/saves", { dataFileUrl: uri });
        var output = { metadata: mldb.get(addr).json.status, config: res.json};
        output;
        """

        filename = 'tmp/MLDB-530-metadata.sqlite'
        try:
            os.mkdir('tmp')
        except OSError:
            # file exists
            try:
                os.unlink(filename)
            except OSError:
                # file doesn't exist
                pass

        mldb.post('/v1/datasets', {
            "id": "recorder",
            "type": "continuous",
            "params": {
                "commitInterval": "0s",
                "metadataDataset": {
                    "type": "sqliteSparse",
                    "id": "metadataDb",
                    "params": {
                        "dataFileUrl": "file://" + filename
                    }
                },
                "createStorageDataset": {
                    "type": "script.run",
                    "params": {
                        "language": "javascript",
                        "scriptConfig": {
                            "source": create_storage_js
                        }
                    }
                },
                "saveStorageDataset": {
                    "type": "script.run",
                    "params": {
                        "language": "javascript",
                        "scriptConfig": {
                            "source": save_storage_js
                        }
                    }
                }
            }
        })

        mldb.post('/v1/datasets/recorder/rows', {
            'rowName' : 'row1',
            'columns' : [['colA', 1, datetime.datetime.now(tzlocal()).isoformat()]]
        })

        datasets = mldb.get('/v1/datasets').json()
        mldb.post('/v1/datasets/recorder/commit')

        mldb.log(mldb.query("SELECT * FROM metadataDb"))

        for ds in datasets:
            if ds not in ['metadataDb', 'recorder']:
                # This simulates the restart of mldb. (The previously commited
                # dataset is no longer loaded.) If you remove it, the test
                # works.
                mldb.delete('/v1/datasets/' + ds)

        # At the moment of creating this test, this call fails with
        # Error initializing continuous window dataset in metadata query:
        # Attempt to refer to nonexistant dataset with id
        # auto-ece4a39e4e8ee8bc-1d16ff0d5362be47
        # (Of course the id is random.)
        mldb.post("/v1/datasets", {
            'id' : 'window',
            "type": "continuous.window",
            "params": {
                "metadataDataset" : {
                    "id": 'metadataDb',
                    "params": {
                        "dataFileUrl": "file://" + filename
                    }
                },
                'from' : '1980-01-01T00:00:00Z',
                'to' : '2020-01-01T00:00:00Z'
            }
        })

        res = mldb.query("SELECT * FROM window")
        self.assertEquals(res[1:], [["row1", 1]])

if __name__ == '__main__':
    mldb.run_tests()
