# Subprocess plugin (EXPERIMENTAL)

This plugin type allows for a plugin to be implemented by an HTTP server
that is started in a separate subprocess from the main MLDB server.

## Configuration

![](%%config plugin experimental.external)

The `commandLine` gives the command used to run the subprocess.  This must
be accessible by MLDB on the local filesystem, for example within the
`mldb_data` directory.

# Subprocess Specification

The subprocess that is started must conform to the following guidelines:

## Startup

The subprocess will be run by MLDB with the command line specified in
the configuration.

## Telling MLDB where the HTTP server is

The subprocess should start an HTTP server, and return to MLDB on standard
output where the port is.  MLDB will wait for up to 5 seconds for this
line to appear, and will return an error if it doesn't.

The line should look like

```
MLDB PLUGIN http http://localhost:1234
```

with the URI telling MLDB what to connect to.  Note that it is not necessary
that the port actually be on localhost; it is possible to ask MLDB to
connect to an external port on the internet (although this is not recommended
due to security).

## Process lifecycle

MLDB will send the process a `SIGHUP` signal when the plugin is deleted or
MLDB shuts down.  The process should respond to this signal by cleaning up
and exiting.  It should continue running indefinitely until it receives
the signal; the behavior is undefined as to what happens if the process
crashes.

## Debugging plugins

To debug a plugin, it is possible to run the plugin outside of the MLDB
container and use normal debugging tools, with a simple script to redirect
MLDB to the external program on startup.

## Routes

The HTTP process should respond to the following routes.

### GET /ping

This method is called by MLDB to check the health of the plugin.  It should
return a 200 if the plugin is OK.

### GET /status

This method is called by MLDB when a GET is called on the plugin itself.  It
should return a JSON payload containing the status of the plugin.

### GET,PUT,POST,DELETE /routes/...

This method is called by MLDB when a custom route is called on the plugin,
ie `/v1/plugins/<id>/routes/...`.  It should implement the route
and return the payload appropriate for that route.

### GET /doc/...

This method is called by MLDB when the documentation for the plugin is
requested.  It should implement the route and return the payload appropriate
for that route.  Often, it suffices to point the subprocess to a static
HTTP server that serves up a directory full of documentation.

## Security

MLDB currently does no authentication of requests to and from subprocess
plugins, and so it is recommended that they only be run locally.

