/* temporary_etcd_server.h                                         -*- C++ -*-
   Jeremy Barnes, 4 March 2014
   Copyright (c) 2014 mldb.ai inc.  All rights reserved.

   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.   
*/

#pragma once

#include "mldb/utils/runner.h"
#include "mldb/io/message_loop.h"
#include "mldb/http/http_rest_proxy.h"
#include "mldb/rest/etcd_client.h"
#include <chrono>
#include <thread>
#include "mldb/jml/utils/string_functions.h"
#include "mldb/jml/utils/info.h"
#include "mldb/utils/command.h"
#include "mldb/vfs/filter_streams.h"
#include "mldb/types/value_description.h"
#include <mutex>

namespace MLDB {

struct TemporaryEtcdServer {

    ~TemporaryEtcdServer()
    {
        stop();
    }

    std::string start(int port = 49000)
    {
        using namespace std;

        stopped = false;

        std::vector<std::string> command = {
            "/usr/bin/docker",
            "run",
            "-t",
            "--rm",
            "-p", std::to_string(port) + ":4001",
            "coreos/etcd"
        };

        auto onTerminate = [&] (const RunResult & result)
            {
                cerr << "etcd service has terminated" << endl;
                std::unique_lock<std::mutex> guard(runResultLock);
                runResult = result;
            };

        cerr << "running " << Command(command).renderShell() << endl;
        
        loop.addSource("runner", runner);
        loop.start();

        runner.run(command, onTerminate, nullptr, nullptr /*stdErrSink*/);

        cerr << "waiting for start" << endl;
        bool started = runner.waitStart();
        cerr << "done waiting for start" << endl;
        
        if (!started) {
            throw MLDB::Exception("error starting etcd: " + jsonEncodeStr(runResult));
        }
        if (!runner.running()) {
            throw MLDB::Exception("service is not running");
        }

        string uri = "http://localhost:" + std::to_string(port);


        for (;;) {
            HttpRestProxy proxy(uri);
            
            try {
                auto resp = proxy.get("/v2/stats/self");
                
                if (resp.code() == 200)
                    break;
                
                cerr << resp << endl;
                
            } catch (...) {
            }

            std::this_thread::sleep_for(std::chrono::milliseconds(500));
        }

        return uri;
    }

    void stop()
    {
        using namespace std;

        if (stopped)
            return;
        if (!runner.running())
            return;

        cerr << "runner is running" << endl;

        stopped = true;

        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        runner.kill();
        runner.waitTermination();
    }

    bool stopped;
    Runner runner;
    MessageLoop loop;
    std::mutex runResultLock;
    RunResult runResult;
};

inline void clearEtcd(const std::string & etcdUri, const std::string & etcdPath)
{
    EtcdClient client(etcdUri, etcdPath);
    auto res = client.eraseDir("", true /* recursive */);
}

inline std::string getEtcdUri()
{
    return "http://localhost:49000";

    static std::string val;

    if (!val.empty())
        return val;

    static TemporaryEtcdServer etcd;
    val = etcd.start();
    return val;
}

inline std::string getEtcdPath()
{
    filter_istream stream("/proc/self/cmdline");
    std::string val = stream.readAll();
    std::string exe = ML::split(val, 0).at(0);
    std::string basename = ML::split(exe, '/').back();
    std::string result = MLDB::username() + "-" + basename;
    return result;
}


} // namespace MLDB
