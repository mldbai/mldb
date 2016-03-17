/** credentials_daemon_test.cc
    Jeremy Barnes, 12 November 2014
    Copyright (c) 2014 Datacratic Inc.  All rights reserved.

    This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

    Test for the credentials daemon.
*/

#include <boost/algorithm/string.hpp>

#include "mldb/credentials/credentials_daemon.h"
#include "mldb/soa/service/remote_credential_provider.h"
#include "mldb/rest/collection_config_store.h"
#include "mldb/soa/service/runner.h"
#include "mldb/soa/service/message_loop.h"
#include "mldb/vfs/filter_streams.h"
#include <boost/algorithm/string.hpp>

#include <future>
#include <chrono>

#define BOOST_TEST_MAIN
#define BOOST_TEST_DYN_LINK

#include <boost/test/unit_test.hpp>

using namespace std;
using namespace Datacratic;

namespace Datacratic {
extern bool disableCloudCredentials;
}

struct CredentialsDaemonUpdater: public CredentialsDaemonClient {
    CredentialsDaemonUpdater(const std::string & uri)
        : CredentialsDaemonClient(uri)
    {
    }

    void deleteAll()
    {
        auto res = conn.perform("DELETE", "/v1/rules");
        cerr << "deleting got " << res << endl;
    }

    void deleteRule(const std::string & ruleName)
    {
        string uri = "/v1/rules/" + ruleName;

        auto res = conn.perform("DELETE", uri);
        cerr << "deleting got " << res << endl;
    }

    void addCredentials(const std::string & ruleName,
                        const std::string & resourceType,
                        const std::string & resource,
                        const std::string & role,
                        Credential credential)
    {
        string uri = "/v1/rules/" + ruleName;

        Datacratic::CredentialRuleConfig rule;
        rule.store.reset(new StoredCredentials);
        rule.store->resourceType = resourceType;
        rule.store->resource = resource;
        rule.store->role = role;
        rule.store->operation = "*";
        rule.store->credential = credential;

        auto res = conn.put(uri, jsonEncode(rule));

        if (res.code() != 201) {
            cerr << res << endl;
            throw ML::Exception("Couldn't add credentials: returned code %d",
                                res.code());
        }
    }
};


struct SubprocessCredentialsdRunner {

    SubprocessCredentialsdRunner()
        : stdin(&runner.getStdInSink()), shutdown(false)
    {
        messageLoop.addSource("runner", runner);
        messageLoop.start();

        vector<string> command = { "./build/x86_64/bin/credentialsd",
                                   "--credentials-path",
                                   "file://tmp/credentials_daemon_test/",
                                   "--listen-port",
                                   "13200-14000",
                                   "--verbose"};
        
        auto onTerminate = std::bind(&SubprocessCredentialsdRunner::commandHasTerminated, this,
                                     std::placeholders::_1);


        std::promise<std::string> gotAddress;

        onStdOut = [&] (const std::string & data)
            {
                vector<string> lines;
                boost::split(lines, data, boost::is_any_of("\n"));

                for (auto & l: lines) {
                    cerr << l << endl;
                    if (l.find("key_id") != string::npos)
                        BOOST_CHECK_MESSAGE(false, "key id leaked to log");

                    if (l.find("key_secret") != string::npos)
                        BOOST_CHECK_MESSAGE(false, "key secret leaked to log");

                    if (l.find("Credentials available on ") != 0)
                        continue;

                    vector<string> fields;
                    boost::split(fields, l, boost::is_any_of(" "));

                    ExcAssertEqual(fields.size(), 4);
                    ExcAssertEqual(fields[0], "Credentials");
                    ExcAssertEqual(fields[1], "available");
                    ExcAssertEqual(fields[2], "on");
                    string uri = fields[3];
                    gotAddress.set_value(uri);
                
                    return;
                }
            };

        //cerr << "running..." << endl;
        runner.run(command,
                   onTerminate,
                   getStdout(),
                   getStdout());
        
        /* Give it 5 seconds to launch */
        bool started = runner.waitStart(5);

        //cerr << "started = " << started << endl;

        if (!started) {
            throw HttpReturnException(500, "Error starting credentialsd subprocess");
        }

        // Now, loop until the process says it's started and returns parameters for
        // how to connect to it

        //cerr << jsonEncode(res);

        // Wait for the function to finish
        std::future<std::string> future = gotAddress.get_future();

        // Give it 15 seconds to initialize
        if (future.wait_for(std::chrono::seconds(15)) != std::future_status::ready) {
            throw HttpReturnException(500, "Error waiting for subprocess initialization");
        }
    
        // Get the address
        try {
            daemonUri = future.get();
        } JML_CATCH_ALL {
            rethrowHttpException(-1, "Error getting subprocess address");
        }

        cerr << "Got external subprocess on address " << daemonUri << endl;
    }

    ~SubprocessCredentialsdRunner()
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
        auto onData = std::bind(&SubprocessCredentialsdRunner::onStdoutData, this,
                                std::placeholders::_1);
        auto onClose = std::bind(&SubprocessCredentialsdRunner::onStdoutClose, this);
        return std::make_shared<CallbackInputSink>(onData, onClose);
    }
};



BOOST_AUTO_TEST_CASE( test_credentials_daemon )
{
    disableCloudCredentials = true;

    int res = system("rm -rf ./tmp/credentials_daemon_test");
    ExcAssertEqual(res, 0);

#if 0
    CredentialsDaemon daemon;

    std::shared_ptr<CollectionConfigStore> configStore;
    configStore.reset(new S3CollectionConfigStore("file://tmp/credentials_daemon_test"));
    configStore->clear();

    daemon.init(configStore);
    std::string daemonUri = daemon.bindTcp(PortRange(18000,19000), "127.0.0.1");
    
    daemon.start();
#else
    SubprocessCredentialsdRunner daemon;
    std::string daemonUri = daemon.daemonUri;
#endif

    CredentialsDaemonUpdater client(daemonUri);

    auto creds = client.getCredentials("aws:s3", "s3://my.bucket/", "default");

    BOOST_CHECK_EQUAL(creds.size(), 0);

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

    Credential cred;
    cred.provider = "Test program";
    cred.protocol = "http";
    cred.location = "s3.amazonaws.com";
    cred.id       = "AK123youandme";
    cred.secret   = "iamasecret";
    cred.validUntil = Date(2030, 1, 1);

    client.addCredentials("mycreds", "aws:s3", "s3://test.bucket/", "", cred);

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

    HttpRestProxy proxy(daemonUri);

    auto key = proxy.get("/v1/rules").jsonBody()[0].asString();

    auto resp = proxy.get("/v1/rules/" + key).jsonBody();

    cerr << resp << endl;

    ExcAssertEqual(resp["stored"]["credential"]["secret"].asString(),
                   "<<credentials removed>>");

    // Remove them
    client.deleteRule("mycreds");
    
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

    auto resp2 = proxy.post("/v1/rules", jsonEncode(s3Creds));
    cerr << resp2 << endl;

    auto resp3 = proxy.get("/v1/rules/mys3creds");
    cerr << resp3 << endl;

    auto resp4 = proxy.get("/v1/types/aws:s3/resources/s3:///credentials");
    cerr << resp4 << endl;

    //daemon.shutdown();
}
