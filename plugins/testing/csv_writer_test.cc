// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/**
 * csv_writer_test.cc
 * Mich, 2015-11-16
 * Copyright (c) 2015 mldb.ai inc. All rights reserved.
 **/

#define BOOST_TEST_MAIN
#define BOOST_TEST_DYN_LINK

#include <boost/test/unit_test.hpp>
#include "mldb/arch/timers.h"
#include "mldb/utils/testing/fixtures.h"
#include "mldb/vfs/fs_utils.h"
#include "mldb/soa/utils/csv_writer.h"


using namespace std;

struct FileHolder{
    string filename;
    FileHolder(string filename) : filename(filename){}
    ~FileHolder(){
            remove(filename.c_str());
        }
};

BOOST_AUTO_TEST_CASE( test_csv_writer )
{
    stringstream ss;
    CsvWriter csv(ss);
    (csv << "a" << "b" << "cde").endl();
    (csv << 4 << 2 << 345).endl();
    BOOST_REQUIRE_EQUAL(ss.str(), "a,b,cde\n"
                                  "4,2,345\n");
}

BOOST_AUTO_TEST_CASE( test_csv_writer_quote_delimiter )
{
    stringstream ss;
    CsvWriter csv(ss, ';', 'o');
    (csv << "a" << "b" << "cod").endl();
    (csv << 4 << 2 << 345).endl();
    BOOST_REQUIRE_EQUAL(ss.str(), "a;b;ocoodo\n"
                                  "4;2;345\n");
}
