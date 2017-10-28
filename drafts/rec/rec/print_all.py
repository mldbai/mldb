import sys
from pprint import pprint
from pandas import DataFrame
from pymldb import Connection
from pymldb.resource import ResourceError
from rec.settings import HOST, PREFIX


def title(title, linechar='-'):
    return '\n' + ' {} '.format(title).center(80, linechar) + '\n'


if __name__ == '__main__':
    if len(sys.argv) > 1:
        whats = sys.argv[1:]
    else:
        whats = ['datasets', 'procedures', 'functions']

    mldb = Connection(HOST)
    prefix = PREFIX
    for what in whats:
        print(title(what.upper(), '='))
        for x in getattr(mldb, what).get():
            if x.startswith(prefix):
                print(title(x))
                print('CONFIG')
                pprint(getattr(mldb, what)(x).get_query())
                if what == 'functions':
                    print('INFO')
                    pprint(getattr(mldb, what)(x).info.get_query())
                if what == 'datasets':
                    try:
                        head = DataFrame(getattr(mldb, what)(x).query.get_query(limit=10, format='aos'))
                    except ResourceError:
                        continue
                    print(head)
                    # pprint(mldb(what)(x).query.get_query(limit=10))
