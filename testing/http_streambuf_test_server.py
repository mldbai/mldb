#!/usr/bin/env python
"""
Source: https://gist.github.com/bradmontgomery/2219997
Very simple HTTP server in python. POST, DELETE and PUT behave the same way:
they simply return HTML content with their verb. GET has 2 behaviours.
1. Calling it with /infinite_redirect will redirect to itself, hence an inifnite
   redirect.
2. Calling it with anything else (/<anything else>) will set <anything else> as
   the response status. It is meant to be used with a valid status code so
   /200, /404, /500, etc.

Usage::
    ./dummy-web-server.py [<port>]

Tips to use it with curl
Send a GET request::
    curl http://localhost
Send a HEAD request::
    curl -I http://localhost
Send a POST request:
    curl -d "foo=bar&bin=baz" http://localhost
"""
from BaseHTTPServer import BaseHTTPRequestHandler, HTTPServer
import SocketServer

class S(BaseHTTPRequestHandler):
    def _set_headers(self):
        self.send_response(200)
        self.send_header('Content-type', 'text/html')
        self.end_headers()

    def do_GET(self):
        if self.path == '/infinite_redirect':
            self.send_response(301)
            self.send_header('Content-type', 'text/html')
            self.send_header('Location', '/infinite_redirect')
            self.end_headers()
            self.wfile.write(
                "<html><body><h1>To infinity and beyond!</h1></body></html>")
            return

        if len(self.path) > 1:
            status = int(self.path[1:])
        else:
            status = 200
        self.send_response(status)
        self.send_header('Content-type', 'text/html')
        self.end_headers()

        self.wfile.write(
            "<html><body><h1>status {}</h1></body></html>".format(status))

    def do_HEAD(self):
        self._set_headers()

    def do_POST(self):
        # Doesn't do anything with posted data
        self._set_headers()
        self.wfile.write("<html><body><h1>POST!</h1></body></html>")

    def do_PUT(self):
        # Doesn't do anything with posted data
        self._set_headers()
        self.wfile.write("<html><body><h1>PUT!</h1></body></html>")

    def do_DELETE(self):
        # Doesn't do anything with posted data
        self._set_headers()
        self.wfile.write("<html><body><h1>DELETE!</h1></body></html>")

def run(server_class=HTTPServer, handler_class=S, port=80):
    server_address = ('', port)
    httpd = server_class(server_address, handler_class)
    print dir(httpd)
    print httpd.server_address
    print 'Starting httpd...'
    httpd.serve_forever()

if __name__ == "__main__":
    from sys import argv

    if len(argv) == 2:
        run(port=int(argv[1]))
    else:
			run()
