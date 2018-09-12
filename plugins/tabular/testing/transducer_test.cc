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
