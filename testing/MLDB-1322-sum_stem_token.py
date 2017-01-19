# This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

#
# MLDB-1322-sum_stem_token.py
# Simon Lemieux, 2016-01-27
# Copyright (c) 2016 mldb.ai inc. All rights reserved.
#

mldb = mldb_wrapper.wrap(mldb) # noqa


import unittest
class SumStemTokenTest(unittest.TestCase):

    def test_sequence(self):

        ds = mldb.create_dataset({
            'type': 'sparse.mutable',
            'id': 'veggies'})
        ds.record_row('row_0', [['txt', 'potato,carrots', 0], ['label',0, 0]])
        ds.record_row('row_1', [['txt', 'potato,potatoes,potato', 0], ['label', 0, 0]])
        ds.record_row('row_2', [['txt', 'carrot,carrots', 0], ['label',1, 0]])
        ds.commit()
        # We have a dataset that looks like this
        #
        #      txt                 label
        #   potato,carrots           0
        # potato,potatoes,potato     0
        # carrot,carrots             1
        #
        # Ultimately I want to count how many of each veggy per label.
        # Let's do it step by step.


        # Step 1: tokenize
        q= '''
        SELECT tokenize(txt) as *, label 
        FROM veggies 
        ORDER BY rowName()
        '''
        res = mldb.query(q)

        mldb.log(res);

        expected = [
            [ "_rowName", "carrots", "label", "potato", "potatoes", "carrot" ], 
            [ "row_0", 1, 0, 1, None, None ], 
            [ "row_1", None, 0, 2, 1, None ], 
            [ "row_2", 1, 1, None, None, 1 ] ]
        assert res == expected

        # Step 2: tokenize and then stem

        mldb.put("/v1/functions/stem", 
            { "type": "stemmer", "params": { "language": "english" } })

        q = '''
        SELECT stem({words: {tokenize(txt) as *}})[words] as *, label
        FROM veggies
        ORDER BY rowName()
        '''
        res = mldb.query(q)

        mldb.log(res)

        expected = [
            ["_rowName", "carrot","label","potato"],
            ["row_0",    1       ,0,1,     ],
            ["row_1",    None    ,0,3,     ],
            ["row_2",    2       ,1,None   ],
        ]
        assert res == expected

        # Step 3: Let start with just summing the columns
        q = '''
        SELECT sum(stem({words: {tokenize(txt) as *}})[words]) as *
        FROM veggies
        '''
        res = mldb.query(q)
        expected = [
            ["_rowName", "carrot","potato"],
            ["[]",          3       ,4]
        ]
        assert res == expected

        # Step 4: same but grouped by label
        q = '''
        SELECT sum(stem({words: {tokenize(txt) as *}})[words]) as *
        FROM veggies
        GROUP BY label
        ORDER BY label
        '''
        res = mldb.query(q)
        expected = [
            ["_rowName", "carrot",  "potato"],
            ["[0]",       1,        4],
            ["[1]",       2,        None]
        ]
        assert res == expected


        # Step 6: same thing but from a merged dataset
        ds = mldb.create_dataset({
            'type': 'sparse.mutable',
            'id': 'label2'})
        ds.record_row('row_0', [['label2',2, 0]])
        ds.record_row('row_1', [['label2', 2, 0]])
        ds.record_row('row_2', [['label2',3, 0]])
        ds.commit()

        q = '''
        SELECT sum(stem({words: tokenize(txt) })[words]) as *
        FROM merge(veggies, label2)
        GROUP BY label2
        ORDER BY label2
        '''
        res = mldb.query(q)
        expected = [
            ["_rowName", "carrot","potato"],
            ["[2]",          1       ,4],
            ["[3]",          2       ,None]
        ]
        assert res == expected

mldb.run_tests()
