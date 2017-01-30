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
    plugin.log("relpath = " + relpath);
    plugin.log("verb = " + verb);
    plugin.log("resource = " + resource);
    plugin.log("params = " + params);
    plugin.log("payload = " + payload);
    plugin.log("contentType = " + contentType);
    plugin.log("contentLength = " + contentLength);
    plugin.log("headers = " + JSON.stringify(headers));

    return { "how": "are you" };
}

plugin.setStatusHandler(handleStatus);
plugin.setRequestHandler(handleRequest);
plugin.serveStaticFolder("/static", "static");

plugin.log("params = " + JSON.stringify(plugin.args));

plugin.log("Hello, world");
