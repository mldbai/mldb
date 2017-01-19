#
# MLDB-2100_fetcher_timeout_test.py
# Francois-Michel L'Heureux, 2016-11-20
# This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.
#
import socket
import threading
import time

class MyThread(threading.Thread):
    def run(self):
        try:
            threading.Thread.run(self)
        except Exception as self.err:
            pass
        else:
            self.err = None

# timeout in case MLDB fails to connect to the socket, the test won't hang
socket.setdefaulttimeout(10)

mldb = mldb_wrapper.wrap(mldb)  # noqa

serversocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
serversocket.bind(('127.0.0.1', 0))
serversocket.listen(1)

port_num = serversocket.getsockname()[1]

keep_going = threading.Event()

def sleeper():
    while not keep_going.is_set():
        time.sleep(1)

def client_thread(clientsocket):
    return threading.Thread(target=sleeper)

def mldb_test():
    mldb.log("MLDB querying")
    res = mldb.query(
        "SELECT fetcher('http://localhost:{}/toto')".format(port_num))
    assert res[1][2].find("Timeout was reached") != -1


mldb_thread = MyThread(target=mldb_test)
mldb_thread.start()

# accept connections from outside
try:
    (clientsocket, address) = serversocket.accept()
except socket.timeout:
    mldb.log("MLDB did not contact the socket")
    raise

# now do something with the clientsocket
# in this case, we'll pretend this is a threaded server
ct = client_thread(clientsocket)
ct.start()
mldb_thread.join()
keep_going.set()
ct.join()
if mldb_thread.err:
    raise mldb_thread.err
mldb.script.set_return("success")
