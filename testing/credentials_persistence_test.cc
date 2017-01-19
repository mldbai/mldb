/** credentials_persistence_test.cc
    Jeremy Barnes, 12 November 2014
    Copyright (c) 2014 mldb.ai inc.  All rights reserved.

    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

    Test for the persistence of credentials.
*/

#include <boost/algorithm/string.hpp>

#include "mldb/server/credential_collection.h"
#include "mldb/rest/collection_config_store.h"
#include "mldb/utils/runner.h"
#include "mldb/io/message_loop.h"
#include "mldb/soa/credentials/credentials.h"
#include "mldb/vfs/filter_streams.h"
#include "mldb/http/http_rest_proxy.h"
#include <boost/algorithm/string.hpp>

#include <future>
#include <chrono>

#define BOOST_TEST_MAIN
#define BOOST_TEST_DYN_LINK

#include <boost/test/unit_test.hpp>

using namespace MLDB;

namespace {
void addCredentialRule(const HttpRestProxy & conn,
                    const MLDB::CredentialRuleConfig & rule)
{
    std::string uri = "/v1/credentials";

    auto res = conn.post(uri, jsonEncode(rule));

    if (res.code() != 201) {
        std::cerr << res << std::endl;
        throw MLDB::Exception("Couldn't add credentials: returned code %d",
                            res.code());
    }
}

#if 0
void deleteAllCredentials(const HttpRestProxy & conn)
{
    auto res = conn.perform("DELETE", "/v1/credentials");
    if (res.code() != 200 && res.code() != 204) {
        std::cerr << res << std::endl;
        throw MLDB::Exception("Couldn't delete credentials: returned code %d",
                            res.code());
    }
}
#endif

void deleteCredentialRule(const HttpRestProxy & conn,
                const std::string & ruleName)
{
    std::string uri = "/v1/credentials/" + ruleName;

    auto res = conn.perform("DELETE", uri);
    if (res.code() != 200 && res.code() != 204) {
        std::cerr << res << std::endl;
        throw MLDB::Exception("Couldn't delete credentials: returned code %d",
                            res.code());
    }
}

} // anonymous namespace

using namespace std;

struct SubprocessMldbRunner {

    SubprocessMldbRunner(const std::string & path)
        : stdin(&runner.getStdInSink()), shutdown(false)
    {
        messageLoop.addSource("runner", runner);
        messageLoop.start();

        vector<string> command = { "./build/x86_64/bin/mldb_runner",
                                   "--credentials-path",
                                   path,
                                   "-p", "13345"};

        auto onTerminate = std::bind(&SubprocessMldbRunner::commandHasTerminated, this,
                                     std::placeholders::_1);

        std::promise<bool> gotReadyMessage;

        onStdOut = [&] (const std::string & data)
            {
                vector<string> lines;
                boost::split(lines, data, boost::is_any_of("\n"));

                for (auto & l: lines) {
                    // cerr << l << endl;
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


        std::future<bool> future = gotReadyMessage.get_future();
        std::future_status status = future.wait_for(std::chrono::seconds(5));
        if (status == std::future_status::timeout)
            throw HttpReturnException(500, "Error starting mldb_runner subprocess. "
                                      "Has the \"MLDB ready\" message changed?");

        cerr << "MLDB ready" << endl;
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
    constexpr const char * credentialsPath = "tmp/credentials_test";

    // make sure we start with a clean folder
    int res = system((string("rm -rf ") + credentialsPath).c_str());
    ExcAssertEqual(res, 0);

    HttpRestProxy conn;
    const std::string baseUri = "http://localhost:13345";
    conn.init(baseUri);

    Credential cred;
    cred.provider = "Test program";
    cred.protocol = "http";
    cred.location = "s3.amazonaws.com";
    cred.id       = "AK123youandme";
    cred.secret   = "iamasecret";
    cred.validUntil = Date(2030, 1, 1);

    MLDB::CredentialRuleConfig rule;
    rule.id = "mycreds";
    rule.store.reset(new StoredCredentials);
    rule.store->resourceType = "aws:s3";
    rule.store->resource = "s3://test.bucket/";
    rule.store->credential = cred;

    {
        // start MLDB
        SubprocessMldbRunner mldb(string("file://") + credentialsPath);

        addCredentialRule(conn, rule);

        // the credential is properly saved to disk
        filter_istream persistStream(string("file://") + credentialsPath + "/mycreds");
        auto loadedCreds = jsonDecodeStream<CredentialRuleConfig>(persistStream);
        BOOST_CHECK_EQUAL(jsonEncode(loadedCreds), jsonEncode(rule));

        // the REST interface returns the newly created creds
        auto key = conn.get("/v1/credentials").jsonBody()[0].asString();
        BOOST_CHECK_EQUAL(key, "mycreds");

        // the REST interface does not leak the credentials
        auto resp = conn.get("/v1/credentials/" + key).jsonBody();
        BOOST_CHECK_EQUAL(resp["stored"]["credential"]["secret"].asString(),
                          "<<credentials removed>>");
    }

    // the credential is still on disk after MLDB has shutdown
    filter_istream persistStream(string("file://") + credentialsPath + "/mycreds");
    auto loadedCreds = jsonDecodeStream<CredentialRuleConfig>(persistStream);
    BOOST_CHECK_EQUAL(jsonEncode(loadedCreds), jsonEncode(rule));

    auto runMldbWithPath = [&conn](const std::string & path)
    {
        // restart MLDB
        SubprocessMldbRunner mldb(path);

        // the credentials have been properly loaded from disk
        auto key = conn.get("/v1/credentials").jsonBody()[0].asString();
        BOOST_CHECK_EQUAL(key, "mycreds");

        // the REST interface does not leak the credentials
        auto resp = conn.get("/v1/credentials/" + key).jsonBody();
        BOOST_CHECK_EQUAL(resp["stored"]["credential"]["secret"].asString(),
                          "<<credentials removed>>");
    };

    runMldbWithPath(string("file://") + credentialsPath);

    // test MLDB robustness against user input

    // file::tmp/credentials_test/
    runMldbWithPath(string("file://") + credentialsPath + "/");

    // tmp/credentials_test
    runMldbWithPath(credentialsPath);

    // tmp/credentials_test/
    runMldbWithPath(string(credentialsPath) + "/");

    {
        // restart MLDB
        SubprocessMldbRunner mldb(string("file://") + credentialsPath);

        // Remove the credentials
        deleteCredentialRule(conn, "mycreds");

        // Check they are no longer there
        try {
            MLDB_TRACE_EXCEPTIONS(false);
            filter_istream persistStream(string("file://") + credentialsPath + "/mycreds");
            auto loadedCreds = jsonDecodeStream<CredentialRuleConfig>(persistStream);
            BOOST_CHECK_MESSAGE(false, "expected the credentials on disk to be deleted");
        } catch (const std::exception & exc) {
            cerr << "reading deleted creds got ***expected error*** " << exc.what() << endl;
        }
    }
}
