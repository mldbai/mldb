import datetime

from mldb import mldb

start = datetime.datetime.now();

# Note that this test requires a *local, uncompresed* copy of the file
# so that it can be memory mapped, and this file is 15GB large and
# contains 150 million records.  So this is not a test that can be
# easily replicated, hence the manual designation.

mldb.put("/v1/procedures/airline", {
    "type":"import.text",
    "params": {
        "dataFileUrl": "file://allyears.1987.2013.csv",
        "offset" : 0,
        "ignoreBadLines" : True,
        "outputDataset": {
            "id": "airline"
        },
        "runOnCreation": True
    }
})
mldb.log(datetime.datetime.now() - start)
