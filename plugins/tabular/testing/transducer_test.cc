/** transducer_test.cc
    Jeremy Barnes, 10 April 2016
    Copyright (c) 2015 mldb.ai inc.  All rights reserved.

    This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.

    Test of coordinate classes.
*/

#include "mldb/plugins/tabular/transducer.h"
#include "mldb/block/memory_region.h"
#include "mldb/utils/hex_dump.h"
#include "mldb/arch/demangle.h"

#define BOOST_TEST_MAIN
#define BOOST_TEST_DYN_LINK

#include <boost/test/unit_test.hpp>
#include "mldb/utils/string_functions.h"

using namespace std;
using namespace MLDB;

BOOST_AUTO_TEST_CASE( test_id_transducer )
{
    std::vector<std::string> vals;
    StringStats stats;

    // Check that 16 bits is really represented in 16 bits
    for (size_t i = 0;  i < 65536;  ++i) {
        string s = format("%05d", i);
        stats.add(s);
        vals.emplace_back(std::move(s));
    }

    MemorySerializer serializer;
    
    std::shared_ptr<StringTransducer> forward, backward;
    std::tie(forward, backward)
        = trainIdTransducer(vals, stats, serializer);

    for (auto & s: vals) {
        size_t len = forward->getOutputLength(s);
        char buf[len];
        string_view enc = forward->generateAll(s, buf, len);

        //cerr << "enc length = " << enc.size() << endl;

        //hex_dump(enc);
        
        char outbuf[s.size()];
        string_view dec = backward->generateAll(enc, outbuf, s.size());

        //hex_dump(dec);
        
        BOOST_REQUIRE_EQUAL(s, dec);
    }
}

BOOST_AUTO_TEST_CASE( test_id_transducer_no_entropy )
{
    std::vector<std::string> vals = { "1" };
    StringStats stats;
    for (auto & s: vals) {
        stats.add(s);
    }
    
    MemorySerializer serializer;
    
    std::shared_ptr<StringTransducer> forward, backward;
    std::tie(forward, backward)
        = trainIdTransducer(vals, stats, serializer);

    //cerr << "forward is " << type_name(*forward) << endl;
    //cerr << "backward is " << type_name(*backward) << endl;
    
    for (auto & s: vals) {
        size_t len = forward->getOutputLength(s);
        char buf[len];
        string_view enc = forward->generateAll(s, buf, len);

        cerr << "enc length = " << enc.size() << endl;

        hex_dump(enc);
        
        char outbuf[s.size()];
        string_view dec = backward->generateAll(enc, outbuf, s.size());
        
        hex_dump(dec);
        
        BOOST_REQUIRE_EQUAL(s, dec);
    }
}

BOOST_AUTO_TEST_CASE( test_id_transducer_empty_string_no_entropy )
{
    std::vector<std::string> vals = { "" };
    StringStats stats;
    for (auto & s: vals) {
        stats.add(s);
    }

    MemorySerializer serializer;
    
    std::shared_ptr<StringTransducer> forward, backward;
    std::tie(forward, backward)
        = trainIdTransducer(vals, stats, serializer);

    for (auto & s: vals) {
        size_t len = forward->getOutputLength(s);
        char buf[len];
        string_view enc = forward->generateAll(s, buf, len);

        //cerr << "enc length = " << enc.size() << endl;

        //hex_dump(enc);
        
        char outbuf[s.size()];
        string_view dec = backward->generateAll(enc, outbuf, s.size());

        //hex_dump(dec);
        
        BOOST_REQUIRE_EQUAL(s, dec);
    }
}

BOOST_AUTO_TEST_CASE( test_id_transducer_no_strings )
{
    std::vector<std::string> vals;
    StringStats stats;

    MemorySerializer serializer;
    
    std::shared_ptr<StringTransducer> forward, backward;

    // Should train, even if it's not ever useful
    std::tie(forward, backward)
        = trainIdTransducer(vals, stats, serializer);

}

BOOST_AUTO_TEST_CASE( test_id_transducer_more_than_64_bits_entropy)
{
    std::vector<std::string> vals = {
        "6c427837-93a6-4bd2-b59e-0c4aacd35c64",
	"6c42ade5-0bc5-4ea4-a4fa-6dab617ce21e",
	"6c431bd0-e7ff-4c45-a2c5-bac026743a90",
	"6c4480ee-f226-4de4-9a99-6c665797c0b2",
	"6c44b0fa-340c-4e50-8df6-1effd934ffc8",
	"00000000-0000-0000-0000-PATATE000000",
	"6c469d1d-ce43-4990-9d76-08e83cf85e6f",
	"6c486dc3-4707-4c04-967e-2f6b27cd6520",
	"6c4921d8-a3ee-434e-9bef-bb64450af455"
    };

    for (size_t len = 4;  len < vals[0].size();  ++len) {

        cerr << "length " << len << endl;
        
        std::vector<std::string> vals2;
        for (auto & v: vals) {
            vals2.emplace_back(string(v, 0, len));
        }
        
        StringStats stats;
        for (auto & s: vals2) {
            stats.add(s);
        }

        MemorySerializer serializer;
        
        std::shared_ptr<StringTransducer> forward, backward;
        std::tie(forward, backward)
            = trainIdTransducer(vals2, stats, serializer);

        for (auto & s: vals2) {
            size_t len = forward->getOutputLength(s);
            char buf[len];
            string_view enc = forward->generateAll(s, buf, len);
            
            //cerr << "enc length = " << enc.size() << endl;
            
            //hex_dump(enc);
            
            char outbuf[s.size()];
            string_view dec = backward->generateAll(enc, outbuf, s.size());

            //hex_dump(dec);
            
            BOOST_REQUIRE_EQUAL(s, dec);
        }
    }
}
