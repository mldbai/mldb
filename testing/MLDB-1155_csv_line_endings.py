# This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

import json, gzip

open_functions = {".csv": open, ".csv.gz": gzip.open}

for ext in open_functions:
    open_function = open_functions[ext]
    # CSV importation should not complain about number of values when missing last column
    with open_function("tmp/csv_newline_missing_last_column"+ext, 'wb') as f:
        f.write("a,b\n")
        f.write("1.0,\n") #NOTE missing value in last position
        f.write("1.0,1.0\n")
        f.write("1.0,\"hello\"\n")

    result = mldb.perform("PUT", "/v1/datasets/x", [], {
        "type": "text.csv.tabular",
        "params": { "dataFileUrl": "file://tmp/csv_newline_missing_last_column"+ext }
    })
    mldb.log(result)

    assert result["statusCode"] == 201
    assert json.loads(result['response'])["status"]["rowCount"] == 3

    result = mldb.perform("GET", "/v1/query", [
        ["q", "select * from x order by cast (rowName() as number)"], ["format", "table"]], {})
    mldb.log(result)
    assert result["statusCode"] == 200
    assert json.loads(result['response'])[3][2] == "hello"

    # CSV importation should not complain about number of values when missing last column
    # with DOS line endings
    with open_function("tmp/csv_newline_missing_last_column_dos"+ext, 'wb') as f:
        f.write("a,b\r\n")
        f.write("1.0,\r\n") #NOTE missing value in last position
        f.write("1.0,1.0\r\n")
        f.write("1.0,\"hello\"\r\n")

    result = mldb.perform("PUT", "/v1/datasets/x", [], {
        "type": "text.csv.tabular",
        "params": { "dataFileUrl": "file://tmp/csv_newline_missing_last_column_dos"+ext }
    })
    mldb.log(result)

    assert result["statusCode"] == 201
    assert json.loads(result['response'])["status"]["rowCount"] == 3

    result = mldb.perform("GET", "/v1/query", [
        ["q", "select * from x order by cast (rowName() as number)"], ["format", "table"]], {})
    mldb.log(result)
    assert result["statusCode"] == 200
    assert json.loads(result['response'])[3][2] == "hello"

    #CSV importation should correctly read float values at the ends of lines
    with open_function("tmp/csv_newline_float_last"+ext, 'wb') as f:
        f.write("a,b\n")
        f.write("1.0,1.0\n")

    result = mldb.perform("PUT", "/v1/datasets/x", [], {
        "type": "text.csv.tabular",
        "params": { "dataFileUrl": "file://tmp/csv_newline_float_last"+ext }
    })
    assert result["statusCode"] == 201
    assert json.loads(result['response'])["status"]["rowCount"] == 1

    result = mldb.perform("GET", "/v1/query", [
        ["q", "select * from x"], ["format", "table"]], {})
    mldb.log(result)
    assert result["statusCode"] == 200
    assert json.loads(result['response'])[1][1] == 1
    assert json.loads(result['response'])[1][2] == 1

    # CSV importation should correctly read float values at the ends of lines
    # with DOS
    with open_function("tmp/csv_newline_float_last_dos"+ext, 'wb') as f:
        f.write("a,b\r\n")
        f.write("1.0,1.0\r\n")

    result = mldb.perform("PUT", "/v1/datasets/x", [], {
        "type": "text.csv.tabular",
        "params": { "dataFileUrl": "file://tmp/csv_newline_float_last_dos"+ext }
    })
    assert result["statusCode"] == 201
    assert json.loads(result['response'])["status"]["rowCount"] == 1

    result = mldb.perform("GET", "/v1/query", [
        ["q", "select * from x"], ["format", "table"]], {})
    mldb.log(result)
    assert result["statusCode"] == 200
    assert json.loads(result['response'])[1][1] == 1
    assert json.loads(result['response'])[1][2] == 1

    #CSV importation should correctly read quoted string values at the ends of lines
    with open_function("tmp/csv_newline_qstring_last"+ext, 'wb') as f:
        f.write("a,b\n")
        f.write("1.0,\"hello\"\n")

    result = mldb.perform("PUT", "/v1/datasets/x", [], {
        "type": "text.csv.tabular",
        "params": { "dataFileUrl": "file://tmp/csv_newline_qstring_last"+ext }
    })
    assert result["statusCode"] == 201
    assert json.loads(result['response'])["status"]["rowCount"] == 1

    result = mldb.perform("GET", "/v1/query", [
        ["q", "select * from x"], ["format", "table"]], {})
    mldb.log(result)
    assert result["statusCode"] == 200
    assert json.loads(result['response'])[1][1] == 1
    assert json.loads(result['response'])[1][2] == "hello"

    # CSV importation should correctly read string values at end of lines
    with open_function("tmp/csv_newline_string_last"+ext, 'wb') as f:
        f.write("a,b\n")
        f.write("1.0,\"hello\"\n")

    result = mldb.perform("PUT", "/v1/datasets/x", [], {
        "type": "text.csv.tabular",
        "params": { "dataFileUrl": "file://tmp/csv_newline_string_last"+ext }
    })
    assert result["statusCode"] == 201
    assert json.loads(result['response'])["status"]["rowCount"] == 1

    result = mldb.perform("GET", "/v1/query", [
        ["q", "select * from x"], ["format", "table"]], {})
    mldb.log(result)
    assert result["statusCode"] == 200
    assert json.loads(result['response'])[1][1] == 1
    assert json.loads(result['response'])[1][2] == "hello"

    # CSV importation should not fail with "bad seek" if missing trailing newline
    with open_function("tmp/csv_newline_missing_last"+ext, 'wb') as f:
        f.write("a,b\n")
        f.write("1.0,1.0") #NOTE no trailing newline on the last line

    result = mldb.perform("PUT", "/v1/datasets/x", [], {
        "type": "text.csv.tabular",
        "params": { "dataFileUrl": "file://tmp/csv_newline_missing_last"+ext }
    })
    mldb.log(result)

    assert result["statusCode"] == 201
    assert json.loads(result['response'])["status"]["rowCount"] == 1

    result = mldb.perform("GET", "/v1/query", [
        ["q", "select * from x"], ["format", "table"]], {})
    mldb.log(result)
    assert result["statusCode"] == 200
    assert json.loads(result['response'])[1][1] == 1
    assert json.loads(result['response'])[1][2] == 1


    # CSV importation should not fail with "bad seek" if missing trailing newline
    # with DOS line endings
    with open_function("tmp/csv_newline_missing_last_dos"+ext, 'wb') as f:
        f.write("a,b\r\n")
        f.write("1.0,1.0") #NOTE no trailing newline on the last line

    result = mldb.perform("PUT", "/v1/datasets/x", [], {
        "type": "text.csv.tabular",
        "params": { "dataFileUrl": "file://tmp/csv_newline_missing_last_dos"+ext }
    })
    mldb.log(result)

    assert result["statusCode"] == 201
    assert json.loads(result['response'])["status"]["rowCount"] == 1

    result = mldb.perform("GET", "/v1/query", [
        ["q", "select * from x"], ["format", "table"]], {})
    mldb.log(result)
    assert result["statusCode"] == 200
    assert json.loads(result['response'])[1][1] == 1
    assert json.loads(result['response'])[1][2] == 1


    # CSV importation should accept a blank trailing line
    with open_function("tmp/csv_newline_trailing_newlines"+ext, 'wb') as f:
        f.write("a,b\n")
        f.write("1.0,1.0\n") 
        f.write("\n");  # NOTE: blank line at end

    result = mldb.perform("PUT", "/v1/datasets/x", [], {
        "type": "text.csv.tabular",
        "params": { "dataFileUrl": "file://tmp/csv_newline_trailing_newlines"+ext }
    })
    mldb.log(result)

    assert result["statusCode"] == 201
    assert json.loads(result['response'])["status"]["rowCount"] == 1

    result = mldb.perform("GET", "/v1/query", [
        ["q", "select * from x"], ["format", "table"]], {})
    mldb.log(result)
    assert result["statusCode"] == 200
    assert json.loads(result['response'])[1][1] == 1
    assert json.loads(result['response'])[1][2] == 1


    # CSV importation should accept a blank trailing line with DOS
    with open_function("tmp/csv_newline_trailing_newlines_dos"+ext, 'wb') as f:
        f.write("a,b\r\n")
        f.write("1.0,1.0\r\n") 
        f.write("\r\n");  # NOTE: blank line at end

    result = mldb.perform("PUT", "/v1/datasets/x", [], {
        "type": "text.csv.tabular",
        "params": { "dataFileUrl": "file://tmp/csv_newline_trailing_newlines_dos"+ext }
    })
    mldb.log(result)

    assert result["statusCode"] == 201
    assert json.loads(result['response'])["status"]["rowCount"] == 1

    result = mldb.perform("GET", "/v1/query", [
        ["q", "select * from x"], ["format", "table"]], {})
    mldb.log(result)
    assert result["statusCode"] == 200
    assert json.loads(result['response'])[1][1] == 1
    assert json.loads(result['response'])[1][2] == 1


mldb.script.set_return("success")
