/** http_client_bench.cc
    Wolfgang Sourdeau, January 2014
    This file is part of MLDB. Copyright 2014 mldb.ai inc. All rights reserved.

    This program benchmarks the HttpClient using different parameters.
*/

#include <atomic>
#include <memory>
#include <string>
#include <thread>

#include <boost/program_options/cmdline.hpp>
#include <boost/program_options/options_description.hpp>
#include <boost/program_options/parsers.hpp>
#include <boost/program_options/variables_map.hpp>

#include "mldb/arch/exception.h"
#include "mldb/types/date.h"
#include "mldb/types/value_description.h"
#include "mldb/types/libc_value_descriptions.h"
#include "mldb/utils/testing/print_utils.h"
#include "mldb/utils/runner.h"
#include "mldb/io/asio_thread_pool.h"
#include "mldb/io/event_loop.h"
#include "mldb/io/legacy_event_loop.h"
#include "mldb/http/http_client.h"
#include "mldb/http/http_rest_proxy.h"
#include "mldb/http/testing/test_http_services.h"

using namespace std;
using namespace MLDB;

enum HttpMethod {
    GET,
    POST,
    PUT
};

/* bench methods */

double
AsyncModelBench(HttpMethod method,
                const string & baseUrl, const string & payload,
                int maxReqs, int concurrency)
{
    int numReqs, numResponses(0), numMissed(0);
    LegacyEventLoop loop;
    loop.start();

    auto onResponse = [&] (const HttpRequest & rq, HttpClientError errorCode_,
                           int status, string && headers, string && body) {
        numResponses++;
        // if (numResponses % 1000) {
            // cerr << "resps: "  + to_string(numResponses) + "\n";
        // }
        if (numResponses == maxReqs) {
            // cerr << "received all responses\n";
            ML::futex_wake(numResponses);
        }
    };
    auto cbs = make_shared<HttpClientSimpleCallbacks>(onResponse);
    HttpRequestContent content(payload, "application/binary");

    HttpClient client(loop, baseUrl, concurrency);
    string url("/");
    Date start = Date::now();
    for (numReqs = 0; numReqs < maxReqs;) {
        bool result;
        if (method == GET) {
            result = client.get(url, cbs);
        }
        else if (method == POST) {
            result = client.post(url, cbs, content);
        }
        else if (method == PUT) {
            result = client.put(url, cbs, content);
        }
        else {
            result = true;
        }
        if (result) {
            numReqs++;
            // if (numReqs % 1000) {
            //     cerr << "reqs: "  + to_string(numReqs) + "\n";
            // }
        }
        else {
            numMissed++;
        }
    }

    while (numResponses < maxReqs) {
        // cerr << (" num Responses: " + to_string(numResponses)
        //          + "; max reqs: " + to_string(maxReqs)
        //          + "\n");
        int old(numResponses);
        ML::futex_wait(numResponses, old);
    }
    Date end = Date::now();

    cerr << "num misses: "  + to_string(numMissed) + "\n";

    return end - start;
}

double
ThreadedModelBench(HttpMethod method,
                   const string & baseUrl, const string & payload,
                   int maxReqs, int concurrency)
{
    vector<thread> threads;

    HttpRestProxy::Content content(payload, "application/binary");

    auto threadFn = [&] (int num, int nReqs) {
        int i;
        HttpRestProxy client(baseUrl);
        for (i = 0; i < nReqs; i++) {
            if (method == GET) {
                auto response = client.get("/");
            }
            else if (method == POST) {
                auto response = client.post("/", content);
            }
            else if (method == PUT) {
                auto response = client.put("/", content);
            }
        }
    };

    Date start = Date::now();
    int slice(maxReqs / concurrency);
    for (int i = 0; i < concurrency; i++) {
        // cerr << "doing slice: "  + to_string(slice) + "\n";
        threads.emplace_back(threadFn, i, slice);
    }
    for (int i = 0; i < concurrency; i++) {
        threads[i].join();
    }

    return Date::now() - start;
}

int main(int argc, char *argv[])
{
    using namespace boost::program_options;

    unsigned int concurrency(0);
    unsigned int serverConcurrency(0);
    int model(0);
    unsigned int maxReqs(0);
    string method("GET");
    unsigned int payloadSize(0);

    string serveriface("127.0.0.1");
    string clientiface(serveriface);

    options_description all_opt;
    all_opt.add_options()
        ("client-iface,C", value(&clientiface),
         "address:port to connect to (\"none\" for no client)")
        ("concurrency,c", value(&concurrency),
         "Number of concurrent requests")
        ("server-concurrency", value(&serverConcurrency),
         "Number of server worker threads (defaults to \"concurrency\")")
        ("method,M", value(&method),
         "Method to use (\"GET\"*, \"PUT\", \"POST\")")
        ("model,m", value(&model),
         "Type of concurrency model (1 for async, 2 for threaded))")
        ("requests,r", value(&maxReqs),
         "total of number of requests to perform")
        ("payload-size,s", value(&payloadSize),
         "size of the response body")
        ("server-iface,S", value(&serveriface),
         "server address (\"none\" for no server)")
        ("help,H", "show help");

    if (argc == 1) {
        return 0;
    }

    variables_map vm;
    store(command_line_parser(argc, argv)
          .options(all_opt)
          .run(),
          vm);
    notify(vm);

    if (vm.count("help")) {
        cerr << all_opt << endl;
        return 1;
    }

    if (concurrency == 0) {
        throw MLDB::Exception("'concurrency' must be specified");
    }
    if (serverConcurrency == 0) {
        serverConcurrency = concurrency;
    }

    if (payloadSize == 0) {
        throw MLDB::Exception("'payload-size' must be specified");
    }

    string payload;
    while (payload.size() < payloadSize) {
        payload += randomString(128);
    }

    /* service setup */
    EventLoop eventLoop;
    TestHttpGetService service(eventLoop);
    AsioThreadPool threadPool(eventLoop);
    threadPool.ensureThreads(serverConcurrency);

    string serviceUrl;
    if (serveriface != "none") {
        cerr << "launching server\n";
        service.portToUse = 20000;

        service.addResponse("GET", "/", 200, payload);
        service.addResponse("PUT", "/", 200, "");
        service.addResponse("POST", "/", 200, "");
        serviceUrl = service.start(serveriface);
    }

    if (clientiface != "none") {
        cerr << "launching client\n";
        if (maxReqs == 0) {
            throw MLDB::Exception("'max-reqs' must be specified");
        }

        if (!(method == "GET" || method == "POST" || method == "PUT")) {
            throw MLDB::Exception("invalid method:" + method);
        }

        string baseUrl;
        if (serveriface != "none") {
            baseUrl = serviceUrl;
        }
        else {
            baseUrl = "http://" + clientiface;
        }

        ::printf("model\tconc.\treqs\tsize\ttime_secs\tBps\tqps\n");

        HttpMethod httpMethod;
        if (method == "GET") {
            httpMethod = GET;
        }
        else if (method == "POST") {
            httpMethod = POST;
        }
        else if (method == "PUT") {
            httpMethod = PUT;
        }
        else {
            throw MLDB::Exception("unknown method: "  + method);
        }

        double delta;
        if (model == 1) {
            delta = AsyncModelBench(httpMethod, baseUrl, payload, maxReqs, concurrency);
        }
        else if (model == 2) {
            delta = ThreadedModelBench(httpMethod, baseUrl, payload, maxReqs, concurrency);
        }
        else {
            throw MLDB::Exception("invalid 'model'");
        }
        double qps = maxReqs / delta;
        double bps = double(maxReqs * payload.size()) / delta;
        ::printf("%d\t%u\t%u\t%u\t%f\t%f\t%f\n",
                 model, concurrency, maxReqs, payloadSize, delta, bps, qps);
    }
    else {
        while (1) {
            sleep(100);
        }
    }

    struct rusage usage;
    getrusage(RUSAGE_SELF, &usage);
    Json::Value jsonUsage = jsonEncode(usage);
    cerr << "rusage:\n" << jsonUsage.toStyledString() << endl;

    return 0;
}
