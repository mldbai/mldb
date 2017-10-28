import sys, zipfile, csv, re, os
from pymldb import Connection
import rec.settings as _
from rec.utils import dataset_conf, load_from_beh_or_import


def load_movies(mldb, fileobj, conf):
    dataset = mldb.create_dataset(conf)

    reader = csv.DictReader(fileobj)
    title_year_regex = re.compile(
        r'^(?P<title>.+) \((?P<year>\d{4})-?(\d{4})?\) ?$')
    for line in reader:
        id = line['movieId']  # keep it as a string for rowName()
        m = title_year_regex.match(line['title'])
        if m is None:
            print('skipping line ', line)
            continue

        title = m.group('title')
        year = int(m.group('year'))
        genres = line['genres'].split('|')
        cols = [['title', title, 0],
                ['year', year, 0],
                # next one is stupid but I wanted a second number
                ['decade', 10*int(str(year)[:-1]), 0],
                ['item_id', id, 0]]  # to work around MLDB-813
        cols.extend([
            ['genre:' + genre, True, 0]
            for genre in genres])
        dataset.rows.post_json({
            'rowName': id,
            'columns': cols})
    dataset.commit.post_json({})


def load_ratings(mldb, fileobj, conf):
    dataset = mldb.create_dataset(conf)

    reader = csv.DictReader(fileobj)
    for i,line in enumerate(reader):
        ts = int(line['timestamp'])
        dataset.rows.post_json({
            'rowName': str(i),
            'columns': [
                ['user_id', line['userId'], ts],
                ['item_id', line['movieId'], ts],
                ['verb', 'rate', ts],
                ['compl', float(line['rating']), ts],
            ]})
    dataset.commit.post_json({})


if __name__ == '__main__':
    # movie lens .zip
    inputfile = sys.argv[1]

    basename = os.path.splitext(os.path.basename(inputfile))[0]
    data_zip = zipfile.ZipFile(inputfile)
    mldb = Connection(_.HOST)

    print('loading movies')
    conf = dataset_conf(_.ITEM_DATASET, 'beh.mutable')
    load_from_beh_or_import(
        mldb, conf, load_movies, mldb, data_zip.open(basename + '/movies.csv'),
        conf)

    print('loading ratings')
    conf = dataset_conf(_.ACTION_DATASET, 'beh.mutable')
    load_from_beh_or_import(
        mldb, conf, load_ratings, mldb,
        data_zip.open(basename + '/ratings.csv'), conf)
