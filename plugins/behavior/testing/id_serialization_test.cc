// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* id_test.cc
   Jeremy Barnes, 17 February 2012
   Copyright (c) 2012 mldb.ai inc.  All rights reserved.

*/

#define BOOST_TEST_MAIN
#define BOOST_TEST_DYN_LINK

#include <boost/test/unit_test.hpp>
#include <iostream>
#include "mldb/plugins/behavior/id.h"
#include "mldb/types/date.h"
#include "mldb/plugins/behavior/id_serialization.h"

using namespace ML;
using namespace std;
using namespace MLDB;

 void checkSerializeReconstitute(Id id)
 {
     BOOST_CHECK_EQUAL(id.toString().size(), id.toStringLength());

     ostringstream oStream;
     {
         DB::Store_Writer oStore(oStream);
         oStore << id;
     }
     
     //cerr << "wrote " << oStream.str().length() << " characters" << endl;
     
     istringstream iStream(oStream.str());
     Id id2;
     {
         DB::Store_Reader iStore(iStream);
         iStore >> id2;
         BOOST_CHECK_EQUAL(iStore.try_to_have(1), 0);
     }
     
     BOOST_CHECK_EQUAL(id.toString(), id2.toString());
     BOOST_CHECK_EQUAL(id, id2);
     BOOST_CHECK_EQUAL(id.type, id2.type);

     BOOST_CHECK_EQUAL(id2.toString().size(), id2.toStringLength());
}

BOOST_AUTO_TEST_CASE( test_basic_id )
{
    Id id;
    id.parse("");
    BOOST_CHECK_EQUAL(id.type, Id::NONE);
    BOOST_CHECK_EQUAL(id.toString(), "");
    checkSerializeReconstitute(id);
}

BOOST_AUTO_TEST_CASE( test_uuid_id )
{
    // lower case
    string uuid = "0828398c-5965-11e0-84c8-0026b937c8e1";
    Id id(uuid);
    BOOST_CHECK_EQUAL(id.type, Id::UUID);
    BOOST_CHECK_EQUAL(id.toString(), uuid);
    checkSerializeReconstitute(id);

    // upper case
    string uuidCaps = "0828398C-5965-11E0-84C8-0026B937C8E1";
    Id idCaps(uuidCaps);
    BOOST_CHECK_EQUAL(idCaps.type, Id::UUID_CAPS);
    BOOST_CHECK_EQUAL(idCaps.toString(), uuidCaps);
    checkSerializeReconstitute(idCaps);
    BOOST_CHECK_NE(idCaps, id);
    BOOST_CHECK_NE(idCaps.hash(), id.hash());

    // mixed case
    string uuidMixed = "0828398C-5965-11e0-84c8-0026b937c8e1";
    Id idMixed(uuidMixed);
    BOOST_CHECK_EQUAL(idMixed.type, Id::STR);
    BOOST_CHECK_EQUAL(idMixed.toString(), uuidMixed);
    checkSerializeReconstitute(idMixed);
    BOOST_CHECK_NE(idMixed, id);
    BOOST_CHECK_NE(idMixed, idCaps);
}

BOOST_AUTO_TEST_CASE( test_goog64_id )
{
    string s = "CAESEAYra3NIxLT9C8twKrzqaA";
    Id id(s);
    BOOST_CHECK_EQUAL(id.type, Id::GOOG128);
    BOOST_CHECK_EQUAL(id.toString(), s);
    checkSerializeReconstitute(id);
}

/* ensures that the upper 64 bits of val are equal to val2 and the lower ones
 * to val1 */
BOOST_AUTO_TEST_CASE( test_int128_64_union_alignment )
{
    Id id;

    id.valHigh = 0x0123456789abcdefLL;
    id.valLow |= 0x1122334455667788;
    BOOST_CHECK_EQUAL(id.val1, 0x1122334455667788);
    BOOST_CHECK_EQUAL(id.val2, 0x0123456789abcdef);
}

BOOST_AUTO_TEST_CASE( test_bigdec_id )
{
    string s = "999999999999";
    Id id(s);
    BOOST_CHECK_EQUAL(id.type, Id::BIGDEC);
    BOOST_CHECK_EQUAL(id.toString(), s);
    checkSerializeReconstitute(id);
}

BOOST_AUTO_TEST_CASE( test_bigdec_id1 )
{
    string s = "7394206091425759590";
    Id id(s);
    BOOST_CHECK_EQUAL(id.type, Id::BIGDEC);
    BOOST_CHECK_EQUAL(id.toString(), s);
    checkSerializeReconstitute(id);
}

BOOST_AUTO_TEST_CASE( test_bigdec_id2 )
{
    string s = "394206091425759590";
    Id id(s);
    BOOST_CHECK_EQUAL(id.type, Id::BIGDEC);
    BOOST_CHECK_EQUAL(id.toString(), s);
    checkSerializeReconstitute(id);
}

BOOST_AUTO_TEST_CASE( test_bigdec_false_positive1 )
{
    string s = "01394206091425759590";
    Id id(s);
    BOOST_CHECK_EQUAL(id.type, Id::STR);
    BOOST_CHECK_EQUAL(id.toString(), s);
    checkSerializeReconstitute(id);
}

BOOST_AUTO_TEST_CASE( test_bigdec_false_positive2 )
{
    string s = "2321323942060989898676554598877575564564435434534354345734371425759590";
    Id id(s);
    BOOST_CHECK_EQUAL(id.type, Id::STR);
    BOOST_CHECK_EQUAL(id.toString(), s);
    checkSerializeReconstitute(id);
}

BOOST_AUTO_TEST_CASE( test_string_id )
{
    string s = "hello";
    Id id(s);
    BOOST_CHECK_EQUAL(id.type, Id::SHORTSTR);
    BOOST_CHECK_EQUAL(id.toString(), s);
    checkSerializeReconstitute(id);
}

BOOST_AUTO_TEST_CASE( test_string_id_copying )
{
    string s = "hello";
    Id id(s);
    id = Id(s);
    BOOST_CHECK_EQUAL(id.type, Id::SHORTSTR);
    BOOST_CHECK_EQUAL(id.toString(), s);
    checkSerializeReconstitute(id);
}

BOOST_AUTO_TEST_CASE( test_long_string_id_copying )
{
    string s = "helloiamaverylongstring";
    Id id(s);
    id = Id(s);
    BOOST_CHECK_EQUAL(id.type, Id::STR);
    BOOST_CHECK_EQUAL(id.toString(), s);
    checkSerializeReconstitute(id);
}

BOOST_AUTO_TEST_CASE( test_null_id )
{
    string s = "null";
    Id id(s);
    id = Id(s);
    BOOST_CHECK_EQUAL(id.type, Id::NULLID);
    BOOST_CHECK_EQUAL(id.toString(), s);
    checkSerializeReconstitute(id);
}

BOOST_AUTO_TEST_CASE( test_empty_id )
{
    string s = "";
    Id id(s);
    id = Id(s);
    BOOST_CHECK_EQUAL(id.type, Id::NONE);
    BOOST_CHECK_EQUAL(id.toString(), s);
    checkSerializeReconstitute(id);
}

BOOST_AUTO_TEST_CASE( test_id_basics )
{
    Id id1("++++++++++++++++");
    BOOST_CHECK_EQUAL(id1.type, Id::BASE64_96);
    BOOST_CHECK_EQUAL(id1.valHigh, 0);
    BOOST_CHECK_EQUAL(id1.valLow, 0);
    
    Id id2("+++++++++++++++/");
    BOOST_CHECK_EQUAL(id2.type, Id::BASE64_96);
    BOOST_CHECK_EQUAL(id2.valHigh, 0);
    BOOST_CHECK_EQUAL(id2.valLow, 1);

    Id id3("+++++++++++++++0");
    BOOST_CHECK_EQUAL(id3.type, Id::BASE64_96);
    BOOST_CHECK_EQUAL(id3.valHigh, 0);
    BOOST_CHECK_EQUAL(id3.valLow, 2);

    Id id4("++++/+++++++++++");
    BOOST_CHECK_EQUAL(id4.type, Id::BASE64_96);
    BOOST_CHECK_EQUAL(id4.valHigh, 1 << 2);
    BOOST_CHECK_EQUAL(id4.valLow, 0);
    BOOST_CHECK_LT(id3, id4);

    BOOST_CHECK_EQUAL(id1.toString().size(), id1.toStringLength());
    BOOST_CHECK_EQUAL(id2.toString().size(), id2.toStringLength());
    BOOST_CHECK_EQUAL(id3.toString().size(), id3.toStringLength());
    BOOST_CHECK_EQUAL(id4.toString().size(), id4.toStringLength());
}

BOOST_AUTO_TEST_CASE( test_id )
{
    string s1 = "++++VpWW999gvYaw";
    string s2 = "+++/uRXa99O0T0+w";
    string s3 = "+++0Rk1K99Oe/3aw";
    string s4 = "jDhUJMWW9997leCw";
    
    Id id1(s1);
    Id id2(s2);
    Id id3(s3);
    Id id4(s4);

    BOOST_CHECK_EQUAL(id1.type, Id::BASE64_96);
    BOOST_CHECK_EQUAL(id2.type, Id::BASE64_96);
    BOOST_CHECK_EQUAL(id3.type, Id::BASE64_96);
    BOOST_CHECK_EQUAL(id4.type, Id::BASE64_96);

    BOOST_CHECK_LT(id1, id2);
    BOOST_CHECK_LT(id2, id3);
    BOOST_CHECK_LT(id3, id4);

    BOOST_CHECK_EQUAL(id1.toString(), s1);
    BOOST_CHECK_EQUAL(id2.toString(), s2);
    BOOST_CHECK_EQUAL(id3.toString(), s3);
    BOOST_CHECK_EQUAL(id4.toString(), s4);

    checkSerializeReconstitute(id1);
    checkSerializeReconstitute(id2);
    checkSerializeReconstitute(id3);
    checkSerializeReconstitute(id4);
}

const char * testBkIds[] = {
    "++++VpWW999gvYaw",
    "+++/uRXa99O0T0+w",
    "+++0Rk1K99Oe/3aw",
    "+++19DxK99YV5GBw",
    "+++19WxK999BtX5w",
    "+++1qAxK99YIIKPm",
    "+++2EAxK99Yu23Nw",
    "+++2VhLR99On4X5w",
    "+++2crRq99OVf1jw",
    "+++5WeWW99ecqwam",
    "+++6cDWc99O02v2w",
    "+++6ulL499YhPo2w",
    "+++7cWxK999Mu1Jw",
    "+++8j/Aa99eKbLjw",
    "+++9/rz599eAuC5w",
    "+++B1vDa99YS4SHw",
    "+++BY06P99evCCOw",
    "+++CjdWj99YfgfHw",
    "+++EBY6K99O2IRJw",
    "+++FYKXj99OuNKjm",
    "+++FaEXq99YFhGkw",
    "+++GYAxK99YkyNjw",
    "+++H9eWW999bv15w",
    "+++HqWxK99YW3z5w",
    "+++IDa1K99Yyta2w",
    "+++IJ06K99YZs1Ow",
    "+++JHEXq99Yn1Hjw",
    "+++JYk1K99eKr15w",
    "+++PnB6D99YIgaHw",
    "+++RPeWW99OJWRBw",
    "+++SDZL499Yg/ajw",
    "+++TWAxK99YHZ22m",
    "+++TwCyE99YcPbOw",
    "+++V5WxK99eQWEPw",
    "+++VUcAU99YCJ98w",
    "+++WDUL499Y48tkw",
    "+++X1GTa999SqChw",
    "+++Xca1s99Ydndam",
    "+++a4CDS9999gJPw",
    "+++bx6Ga99enfDow",
    "+++cK+yK999sjMHw",
    "+++fS/6j99YjJ6Jw",
    "+++hqaXW99OmnE2w",
    "+++iwa9K99919h8w",
    "+++jOtL4999pU3Ow",
    "+++jnDxK99OTXlOw",
    "+++l/a1K999VVz5w",
    "+++mVXAK99OYEz5w",
    "+++mnaXW999YKa2w",
    "+++oPByK99YWgSjw",
    "+++ovRyW99eU2YNw",
    "+++pYk9K999tGtkw",
    "+++pqP1K99eeS6jw",
    "+++pwLRc99YtjEjw",
    "+++t/6xK99Ynh15w",
    "+++tJ3RR999e+Saw",
    "+++vwrRq99ORsX5w",
    "+++xUG6j99e/Xz5w",
    "++/+BJTs99emwkCw",
    "++/+s6xK99ebRZBw",
    "++//VGGP99OXrz5w",
    "++/0DGAa99YQu3kw",
    "++/0au6K99Ort5hm",
    "++/1O8Tq999wO05w",
    "++/1YTTO99Y/nLCw",
    "++/3jN1K99Yq015w",
    "++/4KdWj99Or57Jw",
    "++/4sIRq99O4AU+w",
    "++/7B7L49995IvHw",
    "++/7P56D999o10Ow",
    "++/7cNAa99Y8kz5w",
    "++/8D/Ta99Y25X5w",
    "++/9xTyW99YfSEBw",
    "++/ADUya99eTA3Cw",
    "++/C1EXq99eZyVBw",
    "++/Cq9Tj99YCMX5w",
    "++/Ds06P99OStX5w",
    "++/FPXxs99YHBq8w",
    "++/GD2yK99OF6z5w",
    "++/H9vQO9991pLjw",
    "++/HVR6j99OcczCw",
    "++/JRP9K99O/cXBw",
    "++/JRW/c99eaUj8w",
    "++/JRbQO99YGKzPw",
    "++/Oek9K99e86Maw"
};

int nBkIds = sizeof(testBkIds) / sizeof(testBkIds[0]);

BOOST_AUTO_TEST_CASE( test_id_sorting )
{
    Id prev;
    Id curr;

    for (unsigned i = 0;  i < nBkIds;  ++i) {
        string s = testBkIds[i];
        curr.parse(s);
        BOOST_CHECK_EQUAL(curr.type, Id::BASE64_96);

        BOOST_CHECK_LT(prev, curr);
        
        BOOST_CHECK_EQUAL(curr.toString(), s);
        checkSerializeReconstitute(curr);

        prev = curr;
    }
}
