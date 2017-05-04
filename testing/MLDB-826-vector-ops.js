// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* Full Reddit example, as a serial procedure. */

function assertEqual(expr, val, msg)
{
    if (expr == val)
        return;
    if (JSON.stringify(expr) == JSON.stringify(val))
        return;

    plugin.log("expected", val);
    plugin.log("received", expr);

    throw "Assertion failure: " + msg + ": " + JSON.stringify(expr)
        + " not equal to " + JSON.stringify(val);
}

//smaller tests

mldb.put("/v1/datasets/eg", {"type":"sparse.mutable"})
mldb.post("/v1/datasets/eg/rows", {
    "rowName": "r1", "columns": [ ["a", 10, 0], ["b", 20, 0] ]
})
mldb.post("/v1/datasets/eg/commit", {})

assertEqual(
    mldb.get("/v1/query", {
    q:"select vector_sum([a,b], [a,b]) from eg",
    format:'table', headers: 'false'}).json,
    [["r1",20,40]]
)
assertEqual(
    mldb.get("/v1/query", {
    q:"select vector_sum([a], [b]) from eg",
    format:'table', headers: 'false'}).json,
    [["r1",30]]
)
assertEqual(
    mldb.get("/v1/query", {
    q:"select vector_diff([a,b], [a,b]) from eg",
    format:'table', headers: 'false'}).json,
    [["r1",0,0]]
)
assertEqual(
    mldb.get("/v1/query", {
    q:"select vector_product([a,b], [a,b]) from eg",
    format:'table', headers: 'false'}).json,
    [["r1",100,400]]
)
assertEqual(
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

assertEqual(
    mldb.get("/v1/query", {
    q:"select vector_sum([a1, a2], [b1, b2]) from eg2",
    format:'table', headers: 'false'}).json,
    [["r1",30,55]]
)
assertEqual(
    mldb.get("/v1/query", {
    q:"select vector_diff([a1, a2], [b1, b2]) from eg2",
    format:'table', headers: 'false'}).json,
    [["r1",-10,45]]
)
assertEqual(
    mldb.get("/v1/query", {
    q:"select vector_product([a1, a2], [b1, b2]) from eg2",
    format:'table', headers: 'false'}).json,
    [["r1",200,250]]
)
assertEqual(
    mldb.get("/v1/query", {
    q:"select vector_quotient([a1, a2], [b1, b2]) from eg2",
    format:'table', headers: 'false'}).json,
    [["r1",0.5,10]]
)



"success"
