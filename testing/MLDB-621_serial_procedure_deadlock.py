#
# MLDB-621_serial_procedure_deadlock.py
# datacratic, 2015
# this file is part of mldb. copyright 2015 datacratic. all rights reserved.
#
mldb = mldb_wrapper.wrap(mldb) # noqa

try:
    mldb.perform("PUT", "/v1/procedures/q", [], {
        "type": "serial",
        "params": {"steps": [{"id": "q", "type": "null"}]}
    })
except Exception:
    pass

mldb.script.set_return("success")
