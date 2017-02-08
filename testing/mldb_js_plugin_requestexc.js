// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

// test plugin for MLDB

function handleStatus()
{
    // Hello this is a comment
    plugin.log("handling status");
    return { "message": "A-OK" };
}

function handleRequest(relpath, verb, resource, params, payload, contentType, contentLength,
                       headers)
{
    plugin.log("handling request " + verb + " " + relpath);
    throw "Exception in handleRequest thrown on purpose for testing";
}

plugin.setStatusHandler(handleStatus);
plugin.setRequestHandler(handleRequest);

plugin.log("Hello, world");

