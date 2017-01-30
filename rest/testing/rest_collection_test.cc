// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* rest_collection_test.cc
   Jeremy Barnes, 25 March 2014
   Copyright (c) 2014 mldb.ai inc.  All rights reserved.

*/

#include "mldb/utils/runner.h"
#include "mldb/jml/utils/vector_utils.h"
#include "mldb/rest/rest_collection.h"
#include "mldb/rest/rest_collection_impl.h"
#include "mldb/types/value_description.h"
#include "mldb/rest/cancellation_exception.h"
#include <thread>

#define BOOST_TEST_MAIN
#define BOOST_TEST_DYN_LINK

#include <boost/test/unit_test.hpp>


using namespace std;
using namespace MLDB;

struct TestConfig {
    std::string id;
    std::map<std::string, std::string> params;
};

DECLARE_STRUCTURE_DESCRIPTION(TestConfig);
DEFINE_STRUCTURE_DESCRIPTION(TestConfig);

TestConfigDescription::
TestConfigDescription()
{
    addField("id", &TestConfig::id, "");
    addField("params", &TestConfig::params, "");
}

struct TestStatus {
    std::string id;
    std::string state;
    Json::Value progress;
};

DECLARE_STRUCTURE_DESCRIPTION(TestStatus);
DEFINE_STRUCTURE_DESCRIPTION(TestStatus);

TestStatusDescription::
TestStatusDescription()
{
    addField("id", &TestStatus::id, "");
    addField("state", &TestStatus::state, "");
    addField("progress", &TestStatus::progress, "");
}

struct TestObject {
    std::shared_ptr<TestConfig> config;
};

struct TestCollection
    : public RestConfigurableCollection<std::string, TestObject,
                                        TestConfig, TestStatus> {

    typedef RestConfigurableCollection<std::string, TestObject,
                                       TestConfig, TestStatus> Base;
    
    TestCollection(RestEntity * owner = nullptr)
        : Base("object", "objects", owner ? owner : this)
    {
    }

    TestStatus getStatusFinished(std::string key,
                                 const TestObject & obj) const
    {
        TestStatus result;
        result.state = "ok";
        result.id = key;
        return result;
    }

    TestStatus getStatusLoading(std::string key,
                                const BackgroundTask & task) const
    {
        TestStatus result;
        result.state = "initializing";
        result.id = key;
        result.progress = task.progress;
        return result;
    }

    std::string getKey(TestConfig & config)
    {
        return config.id;
    }

    std::shared_ptr<TestObject>
    construct(TestConfig config, const OnProgress & onProgress) const
    {
        auto result = std::make_shared<TestObject>();
        result->config.reset(new TestConfig(std::move(config)));
        return result;
    }
};


#if 0

BOOST_AUTO_TEST_CASE( test_s3_collection_store )
{
    S3CollectionConfigStore config("s3://tests.datacratic.com/unit_tests/rest_collection_test");
    
    cerr << jsonEncode(config.getAll());

    config.clear();

    BOOST_CHECK_EQUAL(config.keys(), vector<string>());

    config.set("hello", "world");
    cerr << jsonEncode(config.get("hello")) << endl;
    
    BOOST_CHECK_EQUAL(config.get("hello"), "world");

}

BOOST_AUTO_TEST_CASE( test_s3_collection_config_persistence )
{
    // This test makes sure that if we set peristent configuration, create
    // an object and then destroy the collection, when we recreate the
    // collection the same objects are still there.

    auto config = std::make_shared<S3CollectionConfigStore>("s3://tests.datacratic.com/unit_tests/rest_collection_test2");

    // Get rid of anything that was hanging around
    config->clear();

    TestConfig config1{"item1", { { "key1", "value1" } } };
    {
        TestCollection collection;
        collection.attachConfig(config);
        collection.handlePost(config1);

        // Check that it got correctly into the config store
        BOOST_CHECK_EQUAL(collection.getKeys(), vector<string>({"item1"}));
        BOOST_CHECK_EQUAL(config->keys(), vector<string>({"item1"}));
        BOOST_CHECK_EQUAL(config->get("item1"), jsonEncode(config1));
    }

    {
        TestCollection collection;
        collection.attachConfig(config);

        // Check that it correctly loaded up its objects from the config
        // store
        BOOST_CHECK_EQUAL(collection.getKeys(), vector<string>({"item1"}));
        BOOST_CHECK_EQUAL(config->keys(), vector<string>({"item1"}));
        BOOST_CHECK_EQUAL(config->get("item1"), jsonEncode(config1));
    }

    
}
#endif

BOOST_AUTO_TEST_CASE( test_watching )
{
    typedef RestCollection<std::string, std::string> Coll;

    RestDirectory dir(&dir, "dir");
    Coll collection("item", "items", &dir);
    dir.addEntity("items", collection);
    WatchT<Coll::ChildEvent> w = dir.watch({"items", "elements:*"},
                                           true /* catchUp */,
                                           string("w1"));
    WatchT<Coll::ChildEvent> w2 = dir.watch({"items", "elements:test2"},
                                            true /* catchUp */,
                                            string("w2"));
    WatchT<std::vector<Utf8String>, Coll::ChildEvent> w3
        = dir.watchWithPathT<Coll::ChildEvent>({"items", "elements:*"},
                                               true /* catchUp */,
                                               string("w3"));
    
    // Wrong watch type should throw immediately
    {
        MLDB_TRACE_EXCEPTIONS(false);
        BOOST_CHECK_THROW(WatchT<int> w4 = dir.watch({"items", "elements:*"},
                                                     true /* catchUp */,
                                                     string("w4")),
                          std::exception);
    }
        
    BOOST_CHECK(!w.any());

    collection.addEntry("test1", std::make_shared<std::string>("hello1"));

    BOOST_CHECK(w.any());
    BOOST_CHECK(!w2.any());

    auto val = w.pop();

    BOOST_CHECK_EQUAL(val.key, "test1");
    BOOST_CHECK_EQUAL(val.event, CE_NEW);
    BOOST_REQUIRE(val.value);
    BOOST_CHECK_EQUAL(*val.value, "hello1");

    BOOST_CHECK(!w.any());
    
    std::vector<Utf8String> path;
    Coll::ChildEvent ev;
    std::tie(path, ev) = w3.pop();

    BOOST_CHECK_EQUAL(path.size(), 1);
    BOOST_CHECK_EQUAL(path, vector<Utf8String>({"items"}));

    collection.deleteEntry("test1");

    BOOST_CHECK(w.any());
    BOOST_CHECK(!w2.any());

    val = w.pop();

    BOOST_CHECK_EQUAL(val.key, "test1");
    BOOST_CHECK_EQUAL(val.event, CE_DELETED);
    BOOST_REQUIRE(val.value);
    BOOST_CHECK_EQUAL(*val.value, "hello1");

    collection.addEntry("test2", std::make_shared<std::string>("hello2"));

    BOOST_CHECK(w.any());
    BOOST_CHECK(w2.any());

    val = w2.pop();

    BOOST_CHECK_EQUAL(val.key, "test2");
    BOOST_CHECK_EQUAL(val.event, CE_NEW);
    BOOST_REQUIRE(val.value);
    BOOST_CHECK_EQUAL(*val.value, "hello2");

}

struct ConfigColl: public RestConfigurableCollection<std::string, std::string, std::string, std::string> {

    ConfigColl(RestEntity * parent)
        : RestConfigurableCollection<std::string, std::string, std::string, std::string>("item", "items", parent)
    {
    }

    virtual std::string
    getStatusFinished(std::string key, const std::string & value) const
    {
        return value;
    }

    virtual std::string getKey(string & config)
    {
        return config;
    }

    virtual std::shared_ptr<std::string>
    construct(string config, const OnProgress & onProgress) const
    {
        return std::make_shared<std::string>(config);
    }
    
};

BOOST_AUTO_TEST_CASE( test_watching_config )
{
    ConfigColl collection(&collection);

    WatchT<std::string, std::shared_ptr<std::string> > w
        = collection.watch({"config:*"}, true /* catchUp */, string("w"));
#if 0

    WatchT<Coll::ChildEvent> w2 = dir.watch({"items", "elements:test2"},
                                           true /* catchUp */);
    
    // Wrong watch type should throw immediately
    {
        MLDB_TRACE_EXCEPTIONS(false);
        BOOST_CHECK_THROW(WatchT<int> w3 = dir.watch({"items", "elements:*"},
                                                     true /* catchUp */),
                          std::exception);
    }
        
    BOOST_CHECK(!w.any());

    collection.addEntry("test1", std::make_shared<std::string>("hello1"));

    BOOST_CHECK(w.any());
    BOOST_CHECK(!w2.any());

    auto val = w.pop();

    BOOST_CHECK_EQUAL(val.key, "test1");
    BOOST_CHECK_EQUAL(val.event, CE_NEW);
    BOOST_REQUIRE(val.value);
    BOOST_CHECK_EQUAL(*val.value, "hello1");

    BOOST_CHECK(!w.any());

    collection.deleteEntry("test1");

    BOOST_CHECK(w.any());
    BOOST_CHECK(!w2.any());

    val = w.pop();

    BOOST_CHECK_EQUAL(val.key, "test1");
    BOOST_CHECK_EQUAL(val.event, CE_DELETED);
    BOOST_REQUIRE(val.value);
    BOOST_CHECK_EQUAL(*val.value, "hello1");

    collection.addEntry("test2", std::make_shared<std::string>("hello2"));

    BOOST_CHECK(w.any());
    BOOST_CHECK(w2.any());

    val = w2.pop();

    BOOST_CHECK_EQUAL(val.key, "test2");
    BOOST_CHECK_EQUAL(val.event, CE_NEW);
    BOOST_REQUIRE(val.value);
    BOOST_CHECK_EQUAL(*val.value, "hello2");
#endif
}

struct RecursiveCollection: public RestCollection<std::string, RecursiveCollection> {
    RecursiveCollection(const std::string & name,
                        RestEntity * parent)
        : RestCollection<std::string, RecursiveCollection>("item", "items", parent),
          name(name)
    {
    }

    ~RecursiveCollection()
    {
        //cerr << "destroying recursive collection " << name << endl;
    }

    std::pair<const std::type_info *,
              std::shared_ptr<const ValueDescription> >
    getWatchBoundType(const ResourceSpec & spec)
    {
        if (spec.size() > 1) {
            if (spec[0].channel == "children")
                return getWatchBoundType(ResourceSpec(spec.begin() + 1, spec.end()));
            throw MLDB::Exception("only children channel known");
        }
        
        if (spec[0].channel == "children")
            return make_pair(&typeid(std::tuple<RestEntityChildEvent>),
                             nullptr);
        else if (spec[0].channel == "elements")
            return make_pair(&typeid(std::tuple<ChildEvent>), nullptr);
        else throw MLDB::Exception("unknown channel");
    }

    std::string name;
};

BOOST_AUTO_TEST_CASE( test_watching_multi_level )
{
    RecursiveCollection coll("coll", &coll);
    WatchT<RecursiveCollection::ChildEvent> w
        = coll.watch({"*", "elements:*"}, true /* catchUp */, string("w"));

    BOOST_CHECK(!w.any());

    WatchT<std::string> w2;

    // watch is wrong type
    {
        MLDB_TRACE_EXCEPTIONS(false);
        BOOST_CHECK_THROW(w2 = coll.watch({"*", "elements:*"}, true /* catchUp */, string("w2")),
                          std::exception);
    }



    auto coll1 = std::make_shared<RecursiveCollection>("coll1", nullptr);
    auto coll2 = std::make_shared<RecursiveCollection>("coll2", nullptr);

    auto coll11 = std::make_shared<RecursiveCollection>("coll11", coll1.get());
    auto coll12 = std::make_shared<RecursiveCollection>("coll12", coll1.get());
    auto coll21 = std::make_shared<RecursiveCollection>("coll21", coll2.get());
    auto coll22 = std::make_shared<RecursiveCollection>("coll22", coll2.get());

    coll.addEntry("coll1", coll1);

    BOOST_CHECK(!w.any());
    BOOST_CHECK(!w2.any());

    coll1->addEntry("coll11", coll11);
    
    BOOST_CHECK(w.any());

    auto val = w.pop();

    BOOST_CHECK_EQUAL(val.key, "coll11");
    BOOST_CHECK_EQUAL(val.event, CE_NEW);
    BOOST_CHECK_EQUAL(val.value, coll11);

    coll1->addEntry("coll12", coll12);

    BOOST_CHECK(w.any());

    val = w.pop();

    BOOST_CHECK_EQUAL(val.key, "coll12");
    BOOST_CHECK_EQUAL(val.event, CE_NEW);
    BOOST_CHECK_EQUAL(val.value, coll12);

    BOOST_CHECK(!w.any());

    coll1->deleteEntry("coll11");

    BOOST_CHECK(w.any());

    val = w.pop();

    BOOST_CHECK_EQUAL(val.key, "coll11");
    BOOST_CHECK_EQUAL(val.event, CE_DELETED);
    BOOST_CHECK_EQUAL(val.value, coll11);

    BOOST_CHECK(!w.any());

    // Now delete the parent entry.  This should notify us of our child
    // entries disappearing.
    BOOST_CHECK(coll.deleteEntry("coll1"));

    BOOST_CHECK(w.any());

    val = w.pop();

    BOOST_CHECK_EQUAL(val.key, "coll12");
    BOOST_CHECK_EQUAL(val.event, CE_DELETED);
    BOOST_CHECK_EQUAL(val.value, coll12);

    BOOST_CHECK(!w.any());

    // Add a new entry that already has children.  We should get notified
    // of those children immediately that we add it.
    coll2->addEntry("coll21", coll21);
    coll2->addEntry("coll22", coll22);
    coll.addEntry("coll2", coll2);

    BOOST_CHECK(w.any());

    val = w.pop();

    BOOST_CHECK_EQUAL(val.key, "coll21");
    BOOST_CHECK_EQUAL(val.event, CE_NEW);
    BOOST_CHECK_EQUAL(val.value, coll21);

    val = w.pop();

    BOOST_CHECK_EQUAL(val.key, "coll22");
    BOOST_CHECK_EQUAL(val.event, CE_NEW);
    BOOST_CHECK_EQUAL(val.value, coll22);
    
    BOOST_CHECK(!w.any());
}

struct SlowToCreateTestCollection: public TestCollection {

    ~SlowToCreateTestCollection()
    {
        // don't do this to test that we can shutdown from the
        // base class without a pure virtual method call
        //this->shutdown();
    }
    
    std::shared_ptr<TestObject>
    construct(TestConfig config, const OnProgress & onProgress) const
    {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
        auto result = std::make_shared<TestObject>();
        result->config.reset(new TestConfig(std::move(config)));
        return result;
    }
};

// Stress test for MLDB-1259
BOOST_AUTO_TEST_CASE ( test_destroying_while_creating )
{
    int numTests = 100;

    for (unsigned i = 0;  i < numTests;  ++i) {
        cerr << "test " << i << " of " << numTests << endl;
        SlowToCreateTestCollection collection;
        TestConfig config{"item1", {}};
        collection.handlePost("item1", config, true /* must be true */);
        // Destroy it while still being created, to test that we
        // don't crash
    }
}

// Stress test for MLDB-408
BOOST_AUTO_TEST_CASE ( test_cancelling_while_creating )
{
    int numTests = 100;

    for (unsigned i = 0;  i < numTests;  ++i) {
        SlowToCreateTestCollection collection;
        TestConfig config{"item1", {}};
        collection.handlePost("item1", config, true /* must be true */);
        auto entry = collection.getEntry("item1");
        if (entry.second) {
            entry.second->cancel();
            cerr << entry.second->getState() << endl;
        }
        else {
            BOOST_CHECK(false);
        }
    }
}

struct SlowToCreateTestCollectionWithCancellation: public TestCollection {

    ~SlowToCreateTestCollectionWithCancellation()
    {
        this->shutdown();
    }
    
    std::shared_ptr<TestObject>
    construct(TestConfig config, const OnProgress & onProgress) const
    {
        Json::Value progress;
        for (int i = 0; i < 5; ++i) {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
            if (!onProgress(progress)) {
                cerr << "cancelledddddd" << endl;
                throw CancellationException("cancellation");
            }
        }

        auto result = std::make_shared<TestObject>();
        result->config.reset(new TestConfig(std::move(config)));
        return result;
    }
};

/** This is a stress test of the RestCollection, to ensure that cancelling entries does
    not crash MLDB.
*/
BOOST_AUTO_TEST_CASE( stress_test_collection_cancellation )
{
   SlowToCreateTestCollectionWithCancellation collection;
    
    std::atomic<bool> shutdown(false);
    std::atomic<unsigned int> cancelled(0);
    std::atomic<unsigned int> lateCancellation(0);
    std::atomic<unsigned int> created(0);
    std::atomic<unsigned int> underConstruction(0);
    std::atomic<unsigned int> deletedAfterCreation(0);
    std::atomic<unsigned int> cancelledBeforeCreation(0);

     auto addThread = [&] ()
        {
            while (!shutdown) {

                string key = "item2";

                TestConfig config{key, {}};
                
                try {
                    collection.handlePost(key, config, true /* must be new entry */);
                    auto entry = collection.getEntry(key);
                    if (entry.first) {
                        cerr << "entry created" << endl;
                        created++;
                    }
                    else {
                        cerr << "entry is under construction" << endl;
                        underConstruction++;
                    }
                }
                catch (HttpReturnException & ex) {
                    BOOST_ERROR("failed creating an entry or to get "
                                "the entry while under construction");
                }

                std::this_thread::sleep_for(std::chrono::milliseconds(1));

                auto deletedEntry = collection.deleteEntry(key);
                if (deletedEntry)
                    deletedAfterCreation++;
                else
                    cancelledBeforeCreation++;
            };
        };

    auto cancelThread = [&] ()
        {
            while (!shutdown) {

                std::string key = "item2";

                try {
                    auto entry = collection.getEntry(key);
                    if (entry.second) {
                        if (entry.second->cancel())
                            cancelled++;
                    }
                    else {
                        cerr << "failed to cancel the entry because the entry was created" << endl;
                        lateCancellation++;
                    }
                }
                catch (HttpReturnException & ex) {
                    cerr << "failed to get the entry" << endl;
                }
              
                std::this_thread::sleep_for(std::chrono::milliseconds(1));

            };
        };

    std::vector<std::thread> threads;
   
    std::thread creater(addThread);

    unsigned int numCancellationThreads = 10;
    for (unsigned i = 0;  i < numCancellationThreads;  ++i) {
        threads.emplace_back(cancelThread);
    }

    std::this_thread::sleep_for(std::chrono::seconds(1));
    shutdown = true;
    
    for (auto & t: threads)
        t.join();

    creater.join();

    cerr << "created " << created 
         << " under construction " << underConstruction
         << " deleted after creation " << deletedAfterCreation
         << " cancelled before creation " << cancelledBeforeCreation
         << " cancelled " << cancelled
         << " late cancellation " << lateCancellation
         << endl;
    BOOST_CHECK_EQUAL(created + underConstruction, deletedAfterCreation + cancelledBeforeCreation);
}
