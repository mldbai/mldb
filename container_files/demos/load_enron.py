import sys, tarfile, gzip
import requests
from random import randrange
from StringIO import StringIO
from pymldb import Connection

enron_base_url = 'http://www.aueb.gr/users/ion/data/enron-spam/preprocessed/'
enron_data_url = enron_base_url + 'enron{}.tar.gz'

def add_enron_file_to_dataset(mldb, dataset, no, max_msg=None):
    req = requests.get(enron_data_url.format(no))
    if req.status_code != 200:
        raise RuntimeError('enron files not found')
    content = StringIO(req.content)
    gz = gzip.GzipFile(fileobj=content)
    file = tarfile.TarFile(fileobj=gz)

    files = file.getnames()
    ham = sorted([f for f in files if f.endswith('.ham.txt')])
    spam = sorted([f for f in files if f.endswith('.spam.txt')])
    # We insert the spam randomly in the ham, but keeping the ordering. It
    # follows the logic from the article
    where_to_insert = \
        sorted([randrange(len(ham) + 1) for i in xrange(len(spam))])
    # Simply taking into account the fact that the list with get bigger every
    # time we add a new item
    where_to_insert = [x + i for i,x in enumerate(where_to_insert)]

    ham_spam = ham
    for w,s in zip(where_to_insert, spam):
        ham_spam.insert(w, s)

    for i, name in enumerate(ham_spam):
        msg = file.extractfile(name).read()
        # mldb doesn't like non-utf-8 characters, which are present in some
        # mails
        msg = msg.decode('utf-8', 'ignore')
        mldb.post(dataset + '/rows', {
            'rowName': 'mail_' + str(i),
            'columns': [
                ['label', 'spam' if 'spam' in name else 'ham', 0],
                ['index', i, 0],
                ['msg', msg, 0],
                ['dataset', no, 0],
                ['file', name, 0]]})

        if max_msg is not None and i > max_msg:
            break


if __name__ == '__main__':
    # TODO argparse
    mldb = Connection('http://localhost:' + str(sys.argv[1]))
    mldb.put('/v1/datasets/enron_data', {'type': 'sparse.mutable'})

    for i in xrange(1, 7):
        print('Importing Enron ' + str(i))
        add_enron_file_to_dataset(mldb, '/v1/datasets/enron_data', i)
    mldb.post('/v1/datasets/enron_data/commit')
