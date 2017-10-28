import sys
from pymldb import Connection
from rec.settings import HOST, PREFIX


if __name__ == '__main__':
    if len(sys.argv) > 1:
        whats = sys.argv[1:]
    else:
        whats = ['datasets', 'procedures', 'functions']

    mldb = Connection(HOST)
    prefix = PREFIX
    for what in whats:
        for x in getattr(mldb, what).get():
            if x.startswith(prefix):
                print('DELETE ' + str(x))
                getattr(mldb, what)(x).delete()
