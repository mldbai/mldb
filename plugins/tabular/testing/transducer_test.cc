/** path_test.cc
    Jeremy Barnes, 10 April 2016
    Copyright (c) 2015 mldb.ai inc.  All rights reserved.

    This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.

    Test of coordinate classes.
*/

#include "mldb/plugins/tabular/transducer.h"
#include "mldb/block/memory_region.h"
#include "mldb/block/zip_serializer.h"
#include "mldb/base/hex_dump.h"
#include "mldb/utils/environment.h"
#include "mldb/base/scope.h"

#define BOOST_TEST_MAIN
#define BOOST_TEST_DYN_LINK

#include <boost/test/unit_test.hpp>
#include "mldb/utils/string_functions.h"

#pragma clang diagnostic ignored "-Wvla-cxx-extension"

using namespace std;
using namespace MLDB;

static const std::string TMP = Environment::instance()["TMP"];

std::pair<std::shared_ptr<StringTransducer>, std::shared_ptr<StringTransducer>>
testTransducer(const std::vector<std::string> & vals)
{
    MemorySerializer serializer;

    StringStats stats;

    for (auto & v: vals) {
        stats.add(v);
    }
    
    std::shared_ptr<StringTransducer> forward, backward;
    std::tie(forward, backward)
        = trainIdTransducer(vals, stats, serializer);

    for (auto & s: vals) {
        size_t len = forward->getOutputLength(s);
        char buf[len];
        string_view enc = forward->generateAll(s, buf, len);
        char outbuf[s.size()];
        string_view dec = backward->generateAll(enc, outbuf, s.size());
        BOOST_REQUIRE_EQUAL(s, dec);
    }

    string filename = TMP + "/transducer_test.mldbds";

    Scope_Exit(::unlink(filename.c_str()));

    // Test serialization and reconstitution
    {
        ZipStructuredSerializer serializer("file://" + filename);
        forward->serialize(*serializer.newStructure("fwd"));
        backward->serialize(*serializer.newStructure("bwd"));
    }
    
    ZipStructuredReconstituter reconstitutor(Url("file://" + filename));
    auto forward2 = StringTransducer::thaw(*reconstitutor.getStructure("fwd"));
    auto backward2 = StringTransducer::thaw(*reconstitutor.getStructure("bwd"));

    for (auto & s: vals) {
        size_t len = forward->getOutputLength(s);
        char buf[len];
        string_view enc = forward->generateAll(s, buf, len);

        size_t len2 = forward2->getOutputLength(s);
        char buf2[len2];
        string_view enc2 = forward2->generateAll(s, buf2, len2);

        BOOST_CHECK_EQUAL(enc, enc2);

        char outbuf[s.size()];
        string_view dec = backward2->generateAll(enc, outbuf, s.size());
        BOOST_REQUIRE_EQUAL(s, dec);
    }

    return { forward, backward };
}

BOOST_AUTO_TEST_CASE( test_id_transducer_corner_cases )
{
    testTransducer({});
    testTransducer({""});
    testTransducer({"",""});
    testTransducer({"hello"});

    // Test that we can use all 256 characters simultaneously
    std::string s;
    for (int i = 0;  i < 256;  ++i) {
        s.push_back(i);
    }

    testTransducer({s});
}

#if 1
BOOST_AUTO_TEST_CASE(id_transducer_difficult_case_1)
{
    testTransducer({ "OJozXPBBH999gj0PV", "O+9WCkP/e99epmFZ9", "OueEOKa/e99OMCY0/",
                     "OT+yjmK/R999YxGJD", "OfXvjta/e999lOVem", "OC8QFwPWK99Yv4zaD",
                     "OdpGeHtRi99eMDuPV", "ONNyvHPWK99YPXuCV", "O4s126a/e99YpJp/3",
                     "OgN6sEGRL99ObzaJP" });
}
#endif

BOOST_AUTO_TEST_CASE(id_transducer_difficult_case_2)
{
    testTransducer({ "000", "011", "012" });
}

BOOST_AUTO_TEST_CASE(id_transducer_difficult_case_3)
{
    testTransducer({ "012", "011", "000", "010" });
}

BOOST_AUTO_TEST_CASE(id_transducer_difficult_case_4)
{    std::vector<std::string> strings = {
        "ORouter Exception: doStartBidding.alreadyInFlight: auction with ID 6c427837-93a6-4bd2-b59e-0c4aacd35c64 already in progress",
        "ORouter Exception: doStartBidding.alreadyInFlight: auction with ID 6c42ade5-0bc5-4ea4-a4fa-6dab617ce21e already in progress",
        "ORouter Exception: doStartBidding.alreadyInFlight: auction with ID 6c431bd0-e7ff-4c45-a2c5-bac026743a90 already in progress",
        "ORouter Exception: doStartBidding.alreadyInFlight: auction with ID 6c4480ee-f226-4de4-9a99-6c665797c0b2 already in progress",
        "ORouter Exception: doStartBidding.alreadyInFlight: auction with ID 6c44b0fa-340c-4e50-8df6-1effd934ffc8 already in progress",
        "ORouter Exception: doStartBidding.alreadyInFlight: auction with ID 00000000-0000-0000-0000-PATATE000000 already in progress",
        "ORouter Exception: doStartBidding.alreadyInFlight: auction with ID 6c469d1d-ce43-4990-9d76-08e83cf85e6f already in progress",
        "ORouter Exception: doStartBidding.alreadyInFlight: auction with ID 6c486dc3-4707-4c04-967e-2f6b27cd6520 already in progress",
        "ORouter Exception: doStartBidding.alreadyInFlight: auction with ID 6c4921d8-a3ee-434e-9bef-bb64450af455 already in progress"
    };

    testTransducer(strings);
}

BOOST_AUTO_TEST_CASE( test_id_transducer )
{
    std::vector<std::string> vals;

    // Check that 16 bits is really represented in 16 bits
    for (size_t i = 0;  i < 65536;  ++i) {
        string s = format("%05d", i);
        vals.emplace_back(std::move(s));
    }

    testTransducer(vals);
}    
