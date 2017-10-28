How to use the repo
===================

python setup
------------
    
    cd mldb/container_files/drafts/rec
    virtualenv rec-ve
    source rec-ve/bin/activate
    pip install -r requirements.txt

... or at mldb.ai (ask Jean for details)

    pip install -U pip
    pip install --no-index -r requirements.txt

training
--------

For now I have only used the [movielens ml-latest-small.zip dataset][1]. Here
is how to use it to train a model and test it.

    # load the data into MLDB
    MLDB_REC_CONFIG=<your_config.json> python movielens/create_datasets.py <your path to ml-latest-small.zip>
    # train a model!
    MLDB_REC_CONFIG=<your_config.json> python rec/train.py -d 2013-01-01 -n 2000
    # test it
    MLDB_REC_CONFIG=<your_config.json> python rec/test.py

There is a movielens_config_sample.json in the repo. This could in theory kind
of work for BTR by changing the config but it's definitely not quite there yet. 
    

[1]: http://grouplens.org/datasets/movielens/


