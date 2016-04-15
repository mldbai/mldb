/** credentials_persistence_test.cc
    Jeremy Barnes, 12 November 2014
    Copyright (c) 2014 Datacratic Inc.  All rights reserved.

    This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

    Test for the persistence of credentials.
*/

#include <boost/algorithm/string.hpp>

#include "mldb/server/credential_collection.h"
#include "mldb/rest/collection_config_store.h"
#include "mldb/soa/service/runner.h"
#include "mldb/soa/service/message_loop.h"
#include "mldb/vfs/filter_streams.h"
#include "mldb/http/http_rest_proxy.h"
#include <boost/algorithm/string.hpp>

#include <future>
#include <chrono>

#define BOOST_TEST_MAIN
#define BOOST_TEST_DYN_LINK

#include <boost/test/unit_test.hpp>

namespace {
void addCredentials(const std::string & ruleName,
                    const std::string & resourceType,
                    const std::string & resource,
                    const std::string & role,
                    Datacratic::Credential credential,
                    const Datacratic::HttpRestProxy & conn)
{
    std::cerr << "adding credentials" <<std::endl;
    std::string uri = "/v1/credentials/" + ruleName;

    Datacratic::MLDB::CredentialRuleConfig rule;
    rule.store.reset(new Datacratic::MLDB::StoredCredentials);
    rule.store->resourceType = resourceType;
    rule.store->resource = resource;
    rule.store->role = role;
    rule.store->operation = "*";
    rule.store->credential = credential;

    auto res = conn.put(uri, Datacratic::jsonEncode(rule));

    if (res.code() != 201) {
        std::cerr << res << std::endl;
        throw ML::Exception("Couldn't add credentials: returned code %d",
                            res.code());
    }
}

void deleteAll(const Datacratic::HttpRestProxy & conn)
{
    auto res = conn.perform("DELETE", "/v1/rules");
    std::cerr << "deleting got " << res << std::endl;
}

void deleteRule(const Datacratic::HttpRestProxy & conn,
                const std::string & ruleName)
{
    std::string uri = "/v1/rules/" + ruleName;

    auto res = conn.perform("DELETE", uri);
    std::cerr << "deleting got " << res << std::endl;
}

} // anonymous namespace

using namespace std;
using namespace Datacratic;
using namespace Datacratic::MLDB;

struct SubprocessMldbRunner {

    SubprocessMldbRunner()
        : stdin(&runner.getStdInSink()), shutdown(false)
    {
        messageLoop.addSource("runner", runner);
        messageLoop.start();

        vector<string> command = { "./build/x86_64/bin/mldb_runner",
                                   "--credentials-path",
                                   "file://tmp/credentials_test/",
                                   "-p", "13345"};

        auto onTerminate = std::bind(&SubprocessMldbRunner::commandHasTerminated, this,
                                     std::placeholders::_1);

        std::promise<bool> gotReadyMessage;

        onStdOut = [&] (const std::string & data)
            {
                vector<string> lines;
                boost::split(lines, data, boost::is_any_of("\n"));

                for (auto & l: lines) {
                    if (l != "MLDB ready")
                        continue;

                    gotReadyMessage.set_value(true);
                    return;
                }
            };

        cerr << "starting..." << endl;
        runner.run(command,
                   onTerminate,
                   getStdout(),
                   getStdout());

        /* Give it 5 seconds to launch */
        bool started = runner.waitStart(5);

        if (!started) {
            throw HttpReturnException(500, "Error starting mldb_runner subprocess");
        }

        std::future<bool> future = gotReadyMessage.get_future();
        future.get();
        cerr << "ready" << endl;
    }

    ~SubprocessMldbRunner()
    {
        shutdown = true;
        stdin->requestClose();
        runner.kill(SIGTERM, true);
        messageLoop.shutdown();
    }

    Runner runner;
    MessageLoop messageLoop;
    OutputSink * stdin;

    std::mutex mutex;
    RunResult result;

    std::atomic<bool> shutdown;

    std::function<void (const std::string &) > onStdOut;

    std::string daemonUri;

    void commandHasTerminated(const RunResult & result)
    {
        cerr << "terminating" << endl;
        if (shutdown)
            return;
        this->result = result;
    }

    void onStdoutData(std::string && data)
    {
        //cerr << "stdout: " << data << endl;
        if (shutdown)
            return;
        std::unique_lock<std::mutex> guard(mutex);
        if (onStdOut)
            onStdOut(data);
    }

    void onStdoutClose()
    {
    }

    std::shared_ptr<CallbackInputSink> getStdout()
    {
        auto onData = std::bind(&SubprocessMldbRunner::onStdoutData, this,
                                std::placeholders::_1);
        auto onClose = std::bind(&SubprocessMldbRunner::onStdoutClose, this);
        return std::make_shared<CallbackInputSink>(onData, onClose);
    }
};



BOOST_AUTO_TEST_CASE( test_credentials_persistence )
{
    // start MLDB
    SubprocessMldbRunner mldb;
    cerr << "here" << endl;
    HttpRestProxy conn;
    const std::string baseUri = "http://localhost:13345";
    conn.init(baseUri);

    int res = system("rm -rf ./tmp/credentials_test");
    ExcAssertEqual(res, 0);

    // TODO - add test for credential provider
#if 0
    addRemoteCredentialProvider(daemonUri);

    try {
        JML_TRACE_EXCEPTIONS(false);
        filter_istream stream("s3://test.bucket/file.txt");
        BOOST_CHECK(false);
    } catch (const std::exception & exc) {
        cerr << "opening stream got error: " << exc.what() << endl;
        BOOST_CHECK(string(exc.what()).find("No credentials found")
                    != string::npos);
    }
#endif

    Credential cred;
    cred.provider = "Test program";
    cred.protocol = "http";
    cred.location = "s3.amazonaws.com";
    cred.id       = "AK123youandme";
    cred.secret   = "iamasecret";
    cred.validUntil = Date(2030, 1, 1);

    addCredentials("mycreds", "aws:s3", "s3://test.bucket/", "", cred, conn);

#if 0
    try {
        JML_TRACE_EXCEPTIONS(false);
        filter_istream stream("s3://test.bucket/file.txt");
        BOOST_CHECK(false);
    } catch (const std::exception & exc) {
        cerr << "opening stream got error: " << exc.what() << endl;
    }

    filter_istream persistStream("file://tmp/credentials_daemon_test/mycreds");

    auto loadedCreds = jsonDecodeStream<CredentialRuleConfig>(persistStream);

    cerr << "saved creds are " << jsonEncode(loadedCreds) << endl;

    auto key = conn.get("/v1/credentials").jsonBody()[0].asString();

    auto resp = conn.get("/v1/credentials/" + key).jsonBody();

    cerr << resp << endl;

    ExcAssertEqual(resp["stored"]["credential"]["secret"].asString(),
                   "<<credentials removed>>");

    // Remove them
    deleteRule(conn, "mycreds");

    // Check they are no longer there

    try {
        JML_TRACE_EXCEPTIONS(false);
        filter_istream persistStream("file://tmp/credentials_daemon_test/mycreds");
        auto loadedCreds = jsonDecodeStream<CredentialRuleConfig>(persistStream);
        BOOST_CHECK(false);
    } catch (const std::exception & exc) {
        cerr << "reading deleted creds got expected error " << exc.what() << endl;
    }

    auto s3CredsStored = make_shared<StoredCredentials>(StoredCredentials{
        "aws:s3", //type
        "s3://",  //resource
        "",       //role
        "",       //operation
        Date(),   //expiration
        Json::Value(), //extra
        {
            "",     //provider
            "http",
            "s3.amazonaws.com",
            "key_id",
            "key_secret",
            Json::Value(), //extra
            Date() //validUntil
        }
        });

    CredentialRuleConfig s3Creds = {"mys3creds", s3CredsStored};

    auto resp2 = conn.post("/v1/rules", jsonEncode(s3Creds));
    cerr << resp2 << endl;

    auto resp3 = conn.get("/v1/rules/mys3creds");
    cerr << resp3 << endl;

    auto resp4 = conn.get("/v1/types/aws:s3/resources/s3:///credentials");
    cerr << resp4 << endl;
#endif
}
