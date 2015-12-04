# This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

import json

def get_column(result, column):
	assert(result['statusCode'] == 200)
	response = json.loads(result['response'])
	numRow = len(response)
	found = False
	for rowIndex in range (0, numRow):
		row = response[rowIndex]['columns']
		numCol = len(row)
		for colIndex in range (0, numCol):
			if row[colIndex][0] == column:				
				found = True
				return row[colIndex][1];
				break
		if (found):
			break
	assert found

	

dataset_config = {
    'type'    : 'sparse.mutable',
    'id'      : "example"
}

dataset = mldb.create_dataset(dataset_config)

#this is our corpus
dataset.record_row("row1", [ ["test", "peanut butter jelly peanut butter jelly", 0]])
dataset.record_row("row2", [ ["test", "peanut butter jelly time peanut butter jelly time", 0]])
dataset.record_row("row3", [ ["test", "this is the song", 0]])

dataset.commit()

# baggify our words
baggify_conf = {
    "type": "transform",
    "params": {
        "inputDataset": "example",
        "outputDataset": {
            "id": "bag_of_words",
            "type": "sparse.mutable"
        },
        "select": "tokenize(test, {splitchars:' ', quotechar:'', min_token_length: 2}) as *",
    }
}
baggify_output = mldb.perform("PUT", "/v1/procedures/baggify", [], baggify_conf)
mldb.log(baggify_output)

run_output = mldb.perform("POST", "/v1/procedures/baggify/runs")
mldb.log(run_output)

rez = mldb.perform("GET", "/v1/query", [["q", "select * from bag_of_words order by rowName() ASC"]])
mldb.log(rez)

# The procedure will count the number of documents each word appears in
tf_idf_conf = {
    "type": "tfidf.train",
    "params": {
        "trainingDataset": "bag_of_words",
        "outputDataset": {
            "id": "tf_idf",
            "type": "sparse.mutable"
        },
        "functionName" : "tfidffunction"
    }
}

baggify_output = mldb.perform("PUT", "/v1/procedures/tf_idf_proc", [], tf_idf_conf)
mldb.log(baggify_output)

run_output = mldb.perform("POST", "/v1/procedures/tf_idf_proc/runs")
mldb.log(run_output)

rez = mldb.perform("GET", "/v1/query", [["q", "select * from tf_idf order by rowName() ASC"]])
mldb.log(rez)

rez = mldb.perform("GET", "/v1/query", [["q", "select tfidffunction({{*} as input}) as * from tf_idf order by rowName() ASC"]])
mldb.log(rez)

rez = mldb.perform("GET", "/v1/query", [["q", "select tokenize('jelly time', {' ' as splitchars}) as input from tf_idf order by rowName() ASC"]])
mldb.log(rez)

rez = mldb.perform("GET", "/v1/query", [["q", "select tfidffunction({tokenize('jelly time', {' ' as splitchars}) as input}) as * from tf_idf order by rowName() ASC"]])
mldb.log(rez)

#'time' should be more relevant than 'jelly' because its rarer in the corpus
assert get_column(rez, 'output.time') > get_column(rez, 'output.jelly')

func_conf = {
    "type": "tfidf",
    "params": {
        "dataset": "tf_idf",
        "sizeOfCorpus" : 3,
        "tfType" : "augmented",
        "idfType" : "inverseMax",       
    }
}

output = mldb.perform("PUT", "/v1/functions/tfidffunction2", [], func_conf)
mldb.log(output)

rez = mldb.perform("GET", "/v1/query", [["q", "select tfidffunction2({tokenize('jelly time', {' ' as splitchars}) as input}) as * from tf_idf order by rowName() ASC"]])
mldb.log(rez)

#'time' should be more relevant than 'jelly' because its rarer in the corpus
assert get_column(rez, 'output.time') > get_column(rez, 'output.jelly')

mldb.script.set_return("success")