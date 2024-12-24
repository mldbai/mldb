/* test_serialize_reconstitute.h
   Jeremy Barnes, 12 August 2009
   Copyright (c) 2009 Jeremy Barnes.  All rights reserved.
   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

   Testing for the compact size type.
*/

//#pragma once // not here because we can only include this once anyway

#include <sstream>
#include "mldb/types/db/persistent.h"

namespace MLDB {

// NOTE: the macro "CHECK_EQUAL" needs to be defined before this file is included
template<typename X>
void test_serialize_reconstitute(const X & x)
{
    using namespace MLDB::DB;
    using namespace std;

    ostringstream stream_out;

    {
        DB::Store_Writer writer(stream_out);
        writer << x;
        writer << std::string("END");
    }

    istringstream stream_in(stream_out.str());
    
    DB::Store_Reader reader(stream_in);
    X y;
    std::string s;

    try {
        reader >> y;
        reader >> s;
    } catch (const std::exception & exc) {
        cerr << "serialized representation:" << endl;

        string s = stream_out.str();
        for (unsigned i = 0;  i < s.size();  i += 16) {
            cerr << format("%04x | ", i);
            for (unsigned j = i;  j < i + 16;  ++j) {
                if (j < s.size())
                    cerr << format("%02x ", (int)*(unsigned char *)(&s[j]));
                else cerr << "   ";
            }

            cerr << "| ";

            for (unsigned j = i;  j < i + 16;  ++j) {
                if (j < s.size()) {
                    if (s[j] >= ' ' && s[j] <= 127)
                        cerr << s[i];
                    else cerr << '.';
                }
                else cerr << " ";
            }
            cerr << endl;
        }

        throw;
    }

    CHECK_EQUAL(x, y);
    CHECK_EQUAL(s, "END");
}

} // namespace MLDB