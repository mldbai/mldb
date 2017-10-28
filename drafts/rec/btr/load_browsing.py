import csv, re, urlparse, logging, argparse, gzip
from pymldb import Connection
import rec.settings as _
from rec.utils import dataset_conf

log = logging.getLogger(__name__)

btr_url = re.compile(r'^.*beyondtherack\.com$')
becon_url = 'becon-lb.recoset.com'
bluekai_url = 'tags.bluekai.com'

patterns = {
    'event': re.compile(r'^/event/showcase/(?P<event_id>\d+)'),
    'item': re.compile(r'^/event/sku/(?P<event_id>\d+)/(?P<sku>\w+)'),
    # 'bag_confirmation': re.compile(r'^/bag/confirmation'),
    # 'event_calendar': re.compile(r'^/event/calendar'),
    # 'member_friends': re.compile(r'^/member/friends'),
}


def parse_referer(referer):
    """ Prases the referer in sdb and returns a dictionary with a
        type, and some other fields (see the ?P<fields> in the regex).
    """
    data = None
    referer = urlparse.urlparse(referer)
    # Switch on the netloc
    if btr_url.match(referer.netloc):
        # Switch on the path
        for name, regex in patterns.iteritems():
            # For now the %20 is the only such problem I've seen
            # We might want to do something more general if more of those
            # appear
            path = referer.path.replace('%20', '')
            match = regex.match(path)
            if match:
                data = {}
                groupdict = match.groupdict()
                data['type'] = name
                data.update(groupdict)
                break
        else:
            log.debug('url "%s" does not match any pattern',
                      referer.path)
    # elif referer.netloc == becon_url:
    #     return None
        # return {'type': 'becon-lb'}
    # elif referer.netloc == bluekai_url:
    #     return None
        # return {'type': 'tags.bluekai.com'}
    # else:
    #     log.debug('unknown netloc "%s"', referer.netloc)
    #     return None
    return data


def load_sdb(mldb, filename, dataset, nb_lines_max=None, keep_if=None):
    reader = csv.reader(gzip.open(filename))

    if keep_if is None:
        def keep_if(x):
            return True

    import time
    start = time.time()
    nb_done = 0
    nb_rows = 0

    for i, row in enumerate(reader):
        if not keep_if(i):
            continue
        nb_done += 1
        try:
            ts, user_id, url = row
        except ValueError:
            # print(e)
            # print row
            continue

        parsed_url = parse_referer(url)

        if parsed_url is not None:
            ts = int(ts)
            dataset.rows.post_json({
                'rowName': 'pur_' + str(i),
                'columns': [
                    ['user_id', user_id, ts],
                    ['item_id', parsed_url['event_id'], ts],
                    ['action', 'browse', ts]]})
            nb_rows += 1
            if nb_lines_max is not None and nb_rows > nb_lines_max:
                break

        if nb_done % 1e5 == 0:
            dur = time.time() - start
            print 'done {} in {}m @ {:0f}/s'.format(nb_done, dur/60., nb_done/dur)
            # commit once in a while?
            dataset.commit.post_json({})

        if parsed_url is None:
            continue

def load_sdb2(mldb, filename, conf):
    dataset_csv = mldb.create_dataset(
        dataset_conf('temp_tsv', 'text.csv.tabular',
                     maxLines=12,
                     # headers=['ts','user_id','url'],
                     headers=['a','b','c'],
                     dataFileUrl='file://' + filename, encoding='us-ascii'))

if __name__ == '__main__':
    parser = argparse.ArgumentParser('load data from files into mldb')

    tsv_gz_help = '.tsv.gz file, path relative to mldb data dir'
    parser.add_argument('--sdb-dump', required=True, help='the big .gz file')
                        # '(path relative to mldb data dir)')
    parser.add_argument('--users', required=True, help=tsv_gz_help)
    parser.add_argument('--events', required=True, help=tsv_gz_help)
    parser.add_argument('--purchases', required=True, help=tsv_gz_help)
    args = parser.parse_args()

    # logging.basicConfig(level=logging.INFO)

    mldb = Connection(_.HOST)

    dataset = mldb.create_dataset(dataset_conf(_.ACTION_DATASET, 'beh.mutable'))

    from multiprocessing import Pool, Process
    import signal
    signal.signal(signal.SIGINT, signal.SIG_DFL)

    n = 8

    def make_keep_if(i,n):
        def foo(row):
            return row % n == i
        return foo

    # load_sdb(mldb, args.sdb_dump, dataset, None, None)

    processes = []
    for i in xrange(n):
        processes.append(Process(
            target=load_sdb, args=(
                mldb, args.sdb_dump, dataset, None,
                make_keep_if(i,n))))

    for p in processes:
        p.start()
    for p in processes:
        p.join()
