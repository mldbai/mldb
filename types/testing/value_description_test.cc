/* value_description_test.cc                                        -*- C++ -*-
   Wolfgang Sourdeau, June 2013
   Copyright (c) 2013 mldb.ai inc.  All rights reserved.

   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

   Test value_description mechanisms
*/


#define BOOST_TEST_MAIN
#define BOOST_TEST_DYN_LINK
#include <sstream>
#include <string>
#include <boost/lexical_cast.hpp>
#include <boost/test/unit_test.hpp>

#include "mldb/types/basic_value_descriptions.h"
#include "mldb/types/structure_description.h"
#include "mldb/types/map_description.h"
#include "mldb/types/enum_description.h"
#include "mldb/types/vector_description.h"
#include "mldb/types/pointer_description.h"
#include "mldb/types/tuple_description.h"
#include "mldb/types/array_description.h"
#include "mldb/types/date.h"
#include "mldb/base/parse_context.h"
#include "mldb/ext/jsoncpp/json.h"


using namespace std;
using namespace MLDB;


namespace MLDB {

typedef map<string, string> StringDict;

/* SubClass1, where function overloads are used */

struct SubClass1 : public std::string
{
    explicit SubClass1(const std::string & other = "")
        : std::string(other)
    {}
};

PREDECLARE_VALUE_DESCRIPTION(SubClass1);
typedef DescriptionFromBase<SubClass1, std::string> SubClass1Description;
DEFINE_VALUE_DESCRIPTION_NS(SubClass1, SubClass1Description);

typedef map<SubClass1, string> SubClass1Dict;

inline SubClass1 stringToKey(const std::string & str, SubClass1 *) { return SubClass1(str); }
inline std::string keyToString(const SubClass1 & str) { return str; }


/* SubClass2, where template specialization is used */

struct SubClass2 : public std::string
{
    explicit SubClass2(const std::string & other = "")
        : std::string(other)
    {}
};

PREDECLARE_VALUE_DESCRIPTION(SubClass2);
typedef DescriptionFromBase<SubClass2, std::string> SubClass2Description;
DEFINE_VALUE_DESCRIPTION_NS(SubClass2, SubClass2Description);

typedef map<SubClass2, string> SubClass2Dict;

template<typename T>
struct FreeFunctionKeyCodec<SubClass2, T>
{
    static SubClass2 decode(const std::string & s, SubClass2 *) { return SubClass2(s); }
    static std::string encode(const SubClass2 & t) { return t; }
};

/* CompatClass, a class convertible from/to std::string */
struct CompatClass
{
    CompatClass()
    {}

    CompatClass(const std::string & value)
        : value_(value)
    {}

    std::string value_;

    operator std::string() const
    { return value_; }

    bool operator < (const CompatClass & other)
        const
    { return value_ < other.value_; }
};
DECLARE_STRUCTURE_DESCRIPTION(CompatClass);
DEFINE_STRUCTURE_DESCRIPTION(CompatClass);

CompatClassDescription::
CompatClassDescription()
{
    addField("value", &CompatClass::value_, "");
}


typedef map<CompatClass, string> CompatClassDict;

CompatClass stringToKey(const std::string & key, CompatClass *)
{
    return CompatClass(key);
}

string to_string(const CompatClass & k)
{ return string(k); }

}

BOOST_AUTO_TEST_CASE( test_value_description_map )
{
    string data("{ \"key1\": \"value\", \"key2\": \"value2\" }");

    {
        StringDict dict;

        // auto d = getDefaultDescription(&dict);
        // cerr << d << endl;

        dict = jsonDecodeStr(data, &dict);
        BOOST_CHECK_EQUAL(dict["key1"], string("value"));
        BOOST_CHECK_EQUAL(dict["key2"], string("value2"));
    }

    {
        SubClass1Dict dict;

        dict = jsonDecodeStr(data, &dict);
        BOOST_CHECK_EQUAL(dict[SubClass1("key1")], string("value"));
        BOOST_CHECK_EQUAL(dict[SubClass1("key2")], string("value2"));
    }

    {
        SubClass2Dict dict;

        dict = jsonDecodeStr(data, &dict);
        BOOST_CHECK_EQUAL(dict[SubClass2("key1")], string("value"));
        BOOST_CHECK_EQUAL(dict[SubClass2("key2")], string("value2"));
    }

    {
        CompatClassDict dict;

        dict = jsonDecodeStr(data, &dict);

        string value1 = dict[CompatClass("key1")];
        BOOST_CHECK_EQUAL(value1, string("value"));
        string value2 = dict[CompatClass("key2")];
        BOOST_CHECK_EQUAL(value2, string("value2"));
    }

}

enum SomeSize {
    SMALL,
    MEDIUM,
    LARGE
};

DECLARE_ENUM_DESCRIPTION(SomeSize);
DEFINE_ENUM_DESCRIPTION(SomeSize);

SomeSizeDescription::
SomeSizeDescription()
{
    addValue("SMALL", SomeSize::SMALL, "");
    addValue("MEDIUM", SomeSize::MEDIUM, "");
    addValue("LARGE", SomeSize::LARGE, "");
}

struct SomeTestStructure {
    Utf8String someId;
    std::string someText;
    std::vector<std::string> someStringVector;
    SomeSize someSize;

    SomeTestStructure(Utf8String id = "", std::string text = "nothing") : someId(id), someText(text) {
    }

    bool operator==(SomeTestStructure const & other) const {
        return someId == other.someId && someText == other.someText;
    }

    friend std::ostream & operator<<(std::ostream & stream, SomeTestStructure const & data) {
        return stream << "id=" << data.someId << " text=" << data.someText;
    }
};

DEFINE_STRUCTURE_DESCRIPTION(SomeTestStructure)

SomeTestStructureDescription::
SomeTestStructureDescription() {
    addField("someId", &SomeTestStructure::someId, "");
    addField("someText", &SomeTestStructure::someText, "");
    addField("someStringVector", &SomeTestStructure::someStringVector, "");
    addField("someSize", &SomeTestStructure::someSize, "");
}

BOOST_AUTO_TEST_CASE( test_structure_description )
{
    SomeTestStructure data(Utf8String("42"), "hello world");

    // write the thing
    using namespace MLDB;
    ValueDescription * desc = getDefaultDescription(&data);
    std::stringstream stream;
    StreamJsonPrintingContext context(stream);
    desc->printJson(&data, context);

    // inline in some other thing
    std::string value = MLDB::format("{\"%s\":%s}", desc->typeName, stream.str());

    // parse it back
    SomeTestStructure result;
    ParseContext source("test", value.c_str(), value.size());
        expectJsonObject(source, [&](std::string key,
                                     ParseContext & context) {
            auto desc = ValueDescription::get(key);
            if(desc) {
                StreamingJsonParsingContext json(context);
                desc->parseJson(&result, json);
            }
        });

    BOOST_CHECK_EQUAL(result, data);

    std::shared_ptr<const ValueDescription> vd =
        ValueDescription::get("SomeTestStructure");
    BOOST_CHECK_EQUAL(vd->kind, ValueKind::STRUCTURE);

    ValueDescription::FieldDescription fd = vd->getField("someStringVector");
    BOOST_CHECK_EQUAL(fd.description->kind, ValueKind::ARRAY);

    const ValueDescription * subVdPtr = &(fd.description->contained());
    BOOST_CHECK_EQUAL(subVdPtr->kind, ValueKind::STRING);

    fd = vd->getField("someSize");
    BOOST_CHECK_EQUAL(fd.description->kind, ValueKind::ENUM);
    vector<string> keys = fd.description->getEnumKeys();
    BOOST_CHECK_EQUAL(keys.size(), 3);
    BOOST_CHECK_EQUAL(keys[0], "SMALL");
    BOOST_CHECK_EQUAL(keys[1], "MEDIUM");
    BOOST_CHECK_EQUAL(keys[2], "LARGE");
}

struct S1 {
    string val1;
};

struct S2 : S1 {
    string val2;
};

DEFINE_STRUCTURE_DESCRIPTION(S1);
DEFINE_STRUCTURE_DESCRIPTION(S2);

S1Description::S1Description()
{
    addField("val1", &S1::val1, "first value");
}

S2Description::S2Description()
{
    addParent<S1>(); // make sure we don't get "parent description is not a structure
    addField("val2", &S2::val2, "second value");
}

struct RecursiveStructure {
    std::map<std::string, std::shared_ptr<RecursiveStructure> > elements;
    std::vector<std::shared_ptr<RecursiveStructure> > vec;
    std::map<std::string, RecursiveStructure> directElements;
};

DEFINE_STRUCTURE_DESCRIPTION(RecursiveStructure);

RecursiveStructureDescription::RecursiveStructureDescription()
{
    addField("elements", &RecursiveStructure::elements,
             "elements of structure");
    addField("vec", &RecursiveStructure::vec,
             "vector of elements of structure");
    addField("directElements", &RecursiveStructure::directElements,
             "direct map of elements");
}

BOOST_AUTO_TEST_CASE( test_recursive_description )
{
    RecursiveStructureDescription desc;

    RecursiveStructure s;
    s.elements["first"] = make_shared<RecursiveStructure>();
    s.elements["first"]->elements["first.element"] = make_shared<RecursiveStructure>();
    s.elements["first"]->elements["first.element2"];  // null

    s.elements["second"]; // null
    s.vec.push_back(make_shared<RecursiveStructure>());
    s.vec[0]->vec.push_back(make_shared<RecursiveStructure>());
    s.vec[0]->elements["third"] = nullptr;
    s.vec.push_back(nullptr);

    s.directElements["first"].directElements["second"].directElements["third"].vec.push_back(nullptr);

    Json::Value j = jsonEncode(s);

    cerr << j << endl;

    RecursiveStructure s2 = jsonDecode<RecursiveStructure>(j);

    Json::Value j2 = jsonEncode(s2);

    BOOST_CHECK_EQUAL(j, j2);
}

BOOST_AUTO_TEST_CASE( test_date_value_description )
{
    auto desc = getDefaultDescriptionSharedT<Date>();

    Date d = Date::now().quantized(0.001);

    /* timezone is "Z" */
    string isoZ = d.printIso8601();
    BOOST_CHECK_EQUAL(jsonDecode<Date>(isoZ), d);

    /* timezone is "+00:00" */
    string iso00 = isoZ;
    iso00.resize(iso00.size() - 1);
    iso00.append("+00:00");
    BOOST_CHECK_EQUAL(jsonDecode<Date>(iso00), d);

    /* normal (2014-May-02 14:33:02) */
    Date normal = d.quantized(1);
    string normalStr = normal.print();
    cerr << "normalStr = " << normalStr << endl;
    BOOST_CHECK_EQUAL(jsonDecode<Date>(normalStr), normal);
}


/* ensure that struct description invoke struct validators and child
   validators */

int numParentValidations(0);
int numChildValidations(0);

BOOST_AUTO_TEST_CASE( test_date_value_description_validation )
{
    /* test structs */
    struct ParentStruct
    {
        string value;
    };
    struct ChildStruct : public ParentStruct
    {
        int otherValue;
    };

    /* value descriptions */

    struct ParentStructVD
        : public StructureDescriptionImpl<ParentStruct, ParentStructVD>
    {
        ParentStructVD()
        {
            addField("value", &ParentStruct::value, "");
            onPostValidate = [&] (ParentStruct * value,
                                  JsonParsingContext & context) {
                numParentValidations++;
            };
        }
    };
    
    struct ChildStructVD
        : public StructureDescriptionImpl<ChildStruct, ChildStructVD>
    {
        ChildStructVD()
        {
            addParent(new ParentStructVD());
            addField("other-value", &ChildStruct::otherValue, "");
            onPostValidate = [&] (ChildStruct * value,
                                  JsonParsingContext & context) {
                numChildValidations++;
            };
        }
    };

    string testJson("{ \"value\": \"a string value\","
                    "  \"other-value\": 5}");
    ChildStruct testStruct;

    ChildStructVD desc;
    StreamingJsonParsingContext context(testJson,
                                        testJson.c_str(),
                                        testJson.c_str()
                                        + testJson.size());
    desc.parseJson(&testStruct, context);

    BOOST_CHECK_EQUAL(numChildValidations, 1);
    BOOST_CHECK_EQUAL(numParentValidations, 1);
}

// MLDB-1265
BOOST_AUTO_TEST_CASE(test_tuple_description_wrong_length)
{
    TupleDescription<int, string, int> desc;
    std::tuple<int, string, int> testTuple;

    {
        // 4 elements, but only 3 in tuple
        string testJson("[ 1, \"two\", 3, 4 ]");
        StreamingJsonParsingContext context(testJson,
                                            testJson.c_str(),
                                            testJson.c_str()
                                        + testJson.size());
        MLDB_TRACE_EXCEPTIONS(false);
        BOOST_CHECK_THROW(desc.parseJson(&testTuple, context), std::exception);
    }

    {
        // 2 elements, but 3 required in tuple
        string testJson("[ 1, \"two\" ]");
        StreamingJsonParsingContext context(testJson,
                                            testJson.c_str(),
                                            testJson.c_str()
                                        + testJson.size());
        MLDB_TRACE_EXCEPTIONS(false);
        BOOST_CHECK_THROW(desc.parseJson(&testTuple, context), std::exception);
    }

    {
        // 3 elements, wrong type
        string testJson("[ \"one\", \"two\", 3 ]");
        StreamingJsonParsingContext context(testJson,
                                            testJson.c_str(),
                                            testJson.c_str()
                                            + testJson.size());
        MLDB_TRACE_EXCEPTIONS(false);
        BOOST_CHECK_THROW(desc.parseJson(&testTuple, context), std::exception);
    }

    {
        // 3 elements, correct
        string testJson("[ 1, \"two\", 3 ]");
        StreamingJsonParsingContext context(testJson,
                                            testJson.c_str(),
                                            testJson.c_str()
                                            + testJson.size());
        desc.parseJson(&testTuple, context);
        BOOST_CHECK_EQUAL(std::get<0>(testTuple), 1);
        BOOST_CHECK_EQUAL(std::get<1>(testTuple), "two");
        BOOST_CHECK_EQUAL(std::get<2>(testTuple), 3);
    }
}

BOOST_AUTO_TEST_CASE(test_null_parsing)
{
    { 
        // Make sure that nulls result in parsing errors not zeros
        MLDB_TRACE_EXCEPTIONS(false);
        BOOST_CHECK_THROW(jsonDecode<int>(Json::Value()), std::exception);
        BOOST_CHECK_THROW(jsonDecode<unsigned int>(Json::Value()), std::exception);
        BOOST_CHECK_THROW(jsonDecode<long int>(Json::Value()), std::exception);
        BOOST_CHECK_THROW(jsonDecode<unsigned long int>(Json::Value()), std::exception);
        BOOST_CHECK_THROW(jsonDecode<long long int>(Json::Value()), std::exception);
        BOOST_CHECK_THROW(jsonDecode<unsigned long long int>(Json::Value()), std::exception);
        BOOST_CHECK_THROW(jsonDecode<double>(Json::Value()), std::exception);
        BOOST_CHECK_THROW(jsonDecode<float>(Json::Value()), std::exception);
        BOOST_CHECK_THROW(jsonDecode<SomeSize>(Json::Value()), std::exception);

        //BOOST_CHECK_THROW(jsonDecode<unsigned int>(Json::parse("-1")),
        //                  std::exception);

        BOOST_CHECK_THROW(jsonDecodeStr<int>(string("null")), std::exception);
        BOOST_CHECK_THROW(jsonDecodeStr<unsigned int>(string("null")), std::exception);
        BOOST_CHECK_THROW(jsonDecodeStr<long int>(string("null")), std::exception);
        BOOST_CHECK_THROW(jsonDecodeStr<unsigned long int>(string("null")), std::exception);
        BOOST_CHECK_THROW(jsonDecodeStr<long long int>(string("null")), std::exception);
        BOOST_CHECK_THROW(jsonDecodeStr<unsigned long long int>(string("null")), std::exception);
        BOOST_CHECK_THROW(jsonDecodeStr<double>(string("null")), std::exception);
        BOOST_CHECK_THROW(jsonDecodeStr<float>(string("null")), std::exception);
        BOOST_CHECK_THROW(jsonDecodeStr<SomeSize>(string("null")), std::exception);

        //BOOST_CHECK_THROW(jsonDecodeStr<unsigned int>(string("-1")),
        //                  std::exception);
    }

    BOOST_CHECK_EQUAL(jsonDecode<int>(Json::parse("1")), 1);
    BOOST_CHECK_EQUAL(jsonDecode<int>(Json::parse("-1")), -1);

    BOOST_CHECK_EQUAL(jsonDecode<unsigned int>(Json::parse("1")), 1);
}

// MLDB-1265
BOOST_AUTO_TEST_CASE(test_array_description)
{
    ArrayDescription<int, 3> desc;
    std::array<int, 3> testArray;

    {
        // 4 elements, but only 3 in tuple
        string testJson("[ 1, 2, 3, 4 ]");
        StreamingJsonParsingContext context(testJson,
                                            testJson.c_str(),
                                            testJson.c_str()
                                        + testJson.size());
        MLDB_TRACE_EXCEPTIONS(false);
        BOOST_CHECK_THROW(desc.parseJson(&testArray, context), std::exception);
    }

    {
        // 2 elements, but 3 required in tuple
        string testJson("[ 1, 2 ]");
        StreamingJsonParsingContext context(testJson,
                                            testJson.c_str(),
                                            testJson.c_str()
                                        + testJson.size());
        MLDB_TRACE_EXCEPTIONS(false);
        BOOST_CHECK_THROW(desc.parseJson(&testArray, context), std::exception);
    }

    {
        // 3 elements, wrong type
        string testJson("[ \"one\", \"two\", 3 ]");
        StreamingJsonParsingContext context(testJson,
                                            testJson.c_str(),
                                            testJson.c_str()
                                            + testJson.size());
        MLDB_TRACE_EXCEPTIONS(false);
        BOOST_CHECK_THROW(desc.parseJson(&testArray, context), std::exception);
    }

    {
        // 3 elements, correct
        string testJson("[ 1, 2, 3 ]");
        StreamingJsonParsingContext context(testJson,
                                            testJson.c_str(),
                                            testJson.c_str()
                                            + testJson.size());
        desc.parseJson(&testArray, context);
        BOOST_CHECK_EQUAL(std::get<0>(testArray), 1);
        BOOST_CHECK_EQUAL(std::get<1>(testArray), 2);
        BOOST_CHECK_EQUAL(std::get<2>(testArray), 3);
    }
}
