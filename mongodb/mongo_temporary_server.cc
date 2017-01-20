/**                                                                 -*- C++ -*-
 * mongo_temporary_server.cc
 * Mich, 2014-12-15
 * This file is part of MLDB. Copyright 2014 mldb.ai inc. All rights reserved.
 **/


#include <sys/socket.h>
#include <netinet/in.h>
#include "mldb/base/exc_assert.h"
#include "mongo_temporary_server.h"

using namespace std;
using namespace Mongo;
namespace fs = boost::filesystem;
using namespace MLDB;


MongoTemporaryServer::
MongoTemporaryServer(const string & uniquePath, const int portNum)
    : state(Inactive), uniquePath_(uniquePath)
{
    static int index(0);
    ++index;

    if (uniquePath_.empty()) {
        char * tmp = secure_getenv("TMP");
        string tmpDir(tmp == nullptr ? "" : tmp);
        if (tmpDir == "") {
            tmpDir = "./tmp";
        }
        if (tmpDir[0] == '/') {
            // chop the head or mongo will fail with "socket path too long"
            string cwd(secure_getenv("PWD"));
            tmpDir = "." + tmpDir.substr(cwd.size());
        }
        uniquePath_ = MLDB::format("%s/mongo-temporary-server-%d-%d",
                                 tmpDir, getpid(), index);
        cerr << ("starting mongo temporary server under unique path "
                 + uniquePath_ + "\n");
    }

    if (portNum == 0) {
        int freePort = 0;
        {
            struct sockaddr_in addr;
            addr.sin_family = AF_INET;
            auto sockfd = socket(AF_INET, SOCK_STREAM, 0);
            addr.sin_port = 0; //htons(freePort);
            addr.sin_addr.s_addr = INADDR_ANY;
            if (::bind(sockfd, (struct sockaddr *) &addr, sizeof(addr))) {
                throw Exception("Failed to bind on free port");
            }
            socklen_t addrLen = sizeof(addr);
            if (getsockname(sockfd, (struct sockaddr *)&addr, &addrLen) == -1) {
                throw Exception("Failed to getsockname");
            }
            freePort = addr.sin_port;
            close(sockfd);
        }
        this->portNum = freePort;
    }
    else {
        this->portNum = portNum;
    }

    start();
}

MongoTemporaryServer::
~MongoTemporaryServer()
{
    shutdown();
}

void
MongoTemporaryServer::
testConnection()
{
    // 3.  Connect to the server to make sure it works
    int sock = ::socket(AF_UNIX, SOCK_STREAM, 0);
    if (sock == -1) {
        throw MLDB::Exception(errno, "socket");
    }

    sockaddr_un addr;
    addr.sun_family = AF_UNIX;

    // Wait for it to start up
    fs::directory_iterator endItr;
    fs::path socketdir(socketPrefix_);
    bool connected(false);
    for (unsigned i = 0; i < 100 && !connected;  ++i) {
        // read the directory to wait for the socket file to appear
        for (fs::directory_iterator itr(socketdir); itr != endItr; ++itr) {
            ::strcpy(addr.sun_path, itr->path().string().c_str());
            int res = ::connect(sock,
                                (const sockaddr *) &addr, SUN_LEN(&addr));
            if (res == 0) {
                connected = true;
                socketPath_ = itr->path().string();
            }
            else if (res == -1) {
                if (errno != ECONNREFUSED && errno != ENOENT) {
                    throw MLDB::Exception(errno, "connect");
                }
            }
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    if (!connected) {
        throw MLDB::Exception("mongod didn't start up in 10 seconds");
    }
    ::close(sock);
}

void
MongoTemporaryServer::
start()
{
    // Check the unique path
    if (uniquePath_.empty() || uniquePath_ == "." || uniquePath_ == "..") {
        throw MLDB::Exception("unacceptable unique path");
    }

    // 1.  Create the directory

    // First check that it doesn't exist
    struct stat stats;
    int res = ::stat(uniquePath_.c_str(), &stats);
    if (res == -1) {
        if (errno != EEXIST && errno != ENOENT) {
            throw MLDB::Exception(errno, "unhandled exception");
        }
    } else if (res == 0) {
        throw MLDB::Exception("unique path " + uniquePath_ + " already exists");
    }

    cerr << "creating directory " << uniquePath_ << endl;
    if (!fs::create_directories(fs::path(uniquePath_))) {
        throw MLDB::Exception("could not create unique path " + uniquePath_);
    }

    socketPrefix_ = uniquePath_ + "/mongo-socket";
    logfile_ = uniquePath_ + "/output.log";
    int UNIX_PATH_MAX=108;

    if (socketPrefix_.size() >= UNIX_PATH_MAX) {
        throw MLDB::Exception("unix socket path is too long");
    }

    // Create unix socket directory
    fs::path unixdir(socketPrefix_);
    if (!fs::create_directory(unixdir)) {
        throw MLDB::Exception(errno,
                            "couldn't create unix socket directory for Mongo");
    }
    auto onStdOut = [&] (string && message) {
         cerr << "received message on stdout: /" + message + "/" << endl;
        //  receivedStdOut += message;
    };
    auto stdOutSink = make_shared<CallbackInputSink>(onStdOut);

    loop_.addSource("runner", runner_);
    loop_.start();

    auto onTerminate = [&] (const RunResult & result) {
    };
    runner_.run({"/usr/bin/mongod",
                 "--bind_ip", "localhost", "--port", to_string(portNum),
                 "--logpath", logfile_, "--dbpath", uniquePath_,
                 "--unixSocketPrefix", socketPrefix_, "--nojournal"},
                onTerminate, nullptr, stdOutSink);
    // connect to the socket to make sure everything is working fine
    testConnection();
    string payload("db.createUser({user: 'testuser', pwd: 'testpw',"
                                   "roles: ['userAdmin', 'dbAdmin']})");
    RunResult runRes = execute({"/usr/bin/mongo",
                                "localhost:" + to_string(portNum)},
                               nullptr, nullptr, payload);
    ExcAssertEqual(runRes.processStatus(), 0);
    execute({"/usr/bin/mongo", "localhost:" + to_string(portNum)},
                               nullptr, nullptr, "db.getUsers()");

    state = Running;
}

void
MongoTemporaryServer::
suspend()
{
    runner_.kill(SIGSTOP);
    state = Suspended;
}

void
MongoTemporaryServer::
resume()
{
    runner_.kill(SIGCONT);
    state = Running;
}

void
MongoTemporaryServer::
shutdown()
{
    if (runner_.childPid() < 0) {
        return;
    }
    runner_.kill();
    runner_.waitTermination();
    if (uniquePath_ != "") {
        cerr << "removing " << uniquePath_ << endl;
        // throws an exception on error
        fs::remove_all(fs::path(uniquePath_));
        state = Stopped;
    }
}
