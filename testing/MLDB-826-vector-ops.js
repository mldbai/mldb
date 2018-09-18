// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* Full Reddit example, as a serial procedure. */

var mldb = require('mldb')
var unittest = require('mldb/unittest')

//smaller tests

mldb.put("/v1/datasets/eg", {"type":"sparse.mutable"})
mldb.post("/v1/datasets/eg/rows", {
    "rowName": "r1", "columns": [ ["a", 10, 0], ["b", 20, 0] ]
})
mldb.post("/v1/datasets/eg/commit", {})

unittest.assertEqual(
    mldb.get("/v1/query", {
    q:"select vector_sum([a,b], [a,b]) from eg",
    format:'table', headers: 'false'}).json,
    [["r1",20,40]]
)
unittest.assertEqual(
    mldb.get("/v1/query", {
    q:"select vector_sum([a], [b]) from eg",
    format:'table', headers: 'false'}).json,
    [["r1",30]]
)
unittest.assertEqual(
    mldb.get("/v1/query", {
    q:"select vector_diff([a,b], [a,b]) from eg",
    format:'table', headers: 'false'}).json,
    [["r1",0,0]]
)
unittest.assertEqual(
    mldb.get("/v1/query", {
    q:"select vector_product([a,b], [a,b]) from eg",
    format:'table', headers: 'false'}).json,
    [["r1",100,400]]
)
unittest.assertEqual(
    mldb.get("/v1/query", {
    q:"select vector_quotient([a,b], [a,b]) from eg",
    format:'table', headers: 'false'}).json,
    [["r1",1,1]]
)




mldb.put("/v1/datasets/eg2", {"type":"sparse.mutable"})
mldb.post("/v1/datasets/eg2/rows", {
    "rowName": "r1", "columns": [ ["a2", 50, 0], ["b2", 5, 0], ["a1", 10, 0], ["b1", 20, 0] ]
})
mldb.post("/v1/datasets/eg2/commit", {})

unittest.assertEqual(
    mldb.get("/v1/query", {
    q:"select vector_sum([a1, a2], [b1, b2]) from eg2",
    format:'table', headers: 'false'}).json,
    [["r1",30,55]]
)
unittest.assertEqual(
    mldb.get("/v1/query", {
    q:"select vector_diff([a1, a2], [b1, b2]) from eg2",
    format:'table', headers: 'false'}).json,
    [["r1",-10,45]]
)
unittest.assertEqual(
    mldb.get("/v1/query", {
    q:"select vector_product([a1, a2], [b1, b2]) from eg2",
    format:'table', headers: 'false'}).json,
    [["r1",200,250]]
)
unittest.assertEqual(
    mldb.get("/v1/query", {
    q:"select vector_quotient([a1, a2], [b1, b2]) from eg2",
    format:'table', headers: 'false'}).json,
    [["r1",0.5,10]]
)



"success"
