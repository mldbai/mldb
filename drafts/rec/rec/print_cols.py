import sys
from pymldb import Connection
import rec.settings as _
from pprint import pprint

if __name__ == '__main__':
    mldb = Connection(_.HOST)
    if len(sys.argv) <= 1:
        pprint(mldb.datasets.get())
    else:
        dataset_name = sys.argv[1]
        res = mldb.datasets(dataset_name).query.get_query(limit=1, format='table')
        pprint(res[0])
