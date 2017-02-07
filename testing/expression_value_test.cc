/** expression_value_test.cc
    Jeremy Barnes, 1 April 2016
    Copyright (c) 2016 mldb.ai inc.  All rights reserved.

    Tests for the ExpressionValue class.
*/

#include "mldb/sql/expression_value.h"
#include "mldb/types/value_description.h"
#include "mldb/types/vector_description.h"
#include "mldb/types/tuple_description.h"
#include "mldb/jml/stats/distribution.h"
#include "mldb/http/http_exception.h"

#define BOOST_TEST_MAIN
#define BOOST_TEST_DYN_LINK

#include <boost/test/unit_test.hpp>



using namespace std;

using namespace MLDB;

BOOST_AUTO_TEST_CASE( test_size )
{
    BOOST_CHECK_EQUAL(sizeof(ExpressionValue), 32);
}

BOOST_AUTO_TEST_CASE( test_get_embedding_row )
{
    Date ts;

    RowValue row;
    row.emplace_back(PathElement("a"), 1, ts);
    row.emplace_back(PathElement("b"), 2, ts);
        
    ExpressionValue val2(row);

    ColumnPath cols[2] = { PathElement("a"), PathElement("b") };

    auto dist = val2.getEmbedding(cols, 2);

    distribution<double> expected{1, 2};

    BOOST_CHECK_EQUAL(dist, expected);

    ColumnPath cols2[2] = { PathElement("b"), PathElement("a") };

    dist = val2.getEmbedding(cols2, 2);

    distribution<double> expected2{2, 1};

    BOOST_CHECK_EQUAL(dist, expected2);
}

BOOST_AUTO_TEST_CASE( test_unflatten_empty )
{
    std::vector<std::tuple<Path, CellValue, Date> > vals;
    ExpressionValue val(vals);

    BOOST_CHECK_EQUAL(jsonEncodeStr(val), "[[],\"NaD\"]");
}

BOOST_AUTO_TEST_CASE( test_unflatten )
{
    std::vector<std::tuple<Path, CellValue, Date> > vals;
    Date ts;
    vals.emplace_back(PathElement("a"), 1, ts);
    vals.emplace_back(PathElement("b"), 2, ts);

    ExpressionValue val(vals);

    BOOST_CHECK_EQUAL(jsonEncodeStr(val), "[[[\"a\",[1,\"1970-01-01T00:00:00Z\"]],[\"b\",[2,\"1970-01-01T00:00:00Z\"]]],\"1970-01-01T00:00:00Z\"]");
}

BOOST_AUTO_TEST_CASE( test_unflatten_nested )
{
    std::vector<std::tuple<Path, CellValue, Date> > vals;
    Date ts;
    vals.emplace_back(PathElement("a"), 1, ts);
    vals.emplace_back(PathElement("b"), 2, ts);
    vals.emplace_back(PathElement("c") + PathElement("a"), 3, ts);
    vals.emplace_back(PathElement("c") + PathElement("b"), 4, ts);
    vals.emplace_back(PathElement("d"), 5, ts);

    ExpressionValue val(vals);

    BOOST_CHECK_EQUAL(val.extractJson().toStringNoNewLine(),
                      "{\"a\":1,\"b\":2,\"c\":{\"a\":3,\"b\":4},\"d\":5}");
}

BOOST_AUTO_TEST_CASE( test_unflatten_nested_double_val )
{
    std::vector<std::tuple<Path, CellValue, Date> > vals;
    Date ts;
    vals.emplace_back(PathElement("a"), 1, ts);
    vals.emplace_back(PathElement("b"), 2, ts);
    vals.emplace_back(PathElement("c"), 6, ts); // C is both a value and a structure
    vals.emplace_back(PathElement("c") + PathElement("a"), 3, ts);
    vals.emplace_back(PathElement("c") + PathElement("b"), 4, ts);
    vals.emplace_back(PathElement("d"), 5, ts);

    ExpressionValue val(vals);

    std::string expected =
        "[["
        "[\"a\",[ 1, \"1970-01-01T00:00:00Z\" ]	],"
        "[\"b\",[ 2, \"1970-01-01T00:00:00Z\" ]	],"
        "[\"c\",[[[null, [ 6, \"1970-01-01T00:00:00Z\" ]],"
        "         [\"a\",[ 3, \"1970-01-01T00:00:00Z\" ]],"
        "         [\"b\",[ 4, \"1970-01-01T00:00:00Z\" ]]],"
        "         \"1970-01-01T00:00:00Z\"]],"
        "[\"d\",[ 5, \"1970-01-01T00:00:00Z\" ]]],"
	"\"1970-01-01T00:00:00Z\"]";


    BOOST_CHECK_EQUAL(jsonEncode(val),
                      Json::parse(expected));

    // Test that appending it back to a row works
    
    std::vector<std::tuple<Path, CellValue, Date> > vals2;
    Path prefix = PathElement("out");
    val.appendToRow(prefix, vals2);

    expected
        = "["
	"[ \"out.a\", 1, \"1970-01-01T00:00:00Z\" ],"
	"[ \"out.b\", 2, \"1970-01-01T00:00:00Z\" ],"
	"[ \"out.c\", 6, \"1970-01-01T00:00:00Z\" ],"
	"[ \"out.c.a\", 3, \"1970-01-01T00:00:00Z\" ],"
	"[ \"out.c.b\", 4, \"1970-01-01T00:00:00Z\" ],"
	"[ \"out.d\", 5, \"1970-01-01T00:00:00Z\" ] ]";

    BOOST_CHECK_EQUAL(jsonEncode(vals2), Json::parse(expected));

    vals2.clear();
    val.appendToRowDestructive(prefix, vals2);

    BOOST_CHECK_EQUAL(jsonEncode(vals2), Json::parse(expected));
}

BOOST_AUTO_TEST_CASE( test_unflatten_nested_bad_order )
{
    std::vector<std::tuple<Path, CellValue, Date> > vals;
    Date ts;
    PathElement a("a"), b("b");
    vals.emplace_back(a + a, 1, ts);
    vals.emplace_back(b + a, 2, ts);
    vals.emplace_back(a + b, 3, ts);
    vals.emplace_back(b + b, 4, ts);

    ExpressionValue val(vals);

    BOOST_CHECK_EQUAL(jsonEncode(val),
                      Json::parse("[[[\"a\",[[[\"a\",[1,\"1970-01-01T00:00:00Z\"]],"
                                            "[\"b\",[3,\"1970-01-01T00:00:00Z\"]]],"
                                    "\"1970-01-01T00:00:00Z\"]],"
                                   "[\"b\",[[[\"a\",[2,\"1970-01-01T00:00:00Z\"]],"
                                            "[\"b\",[4,\"1970-01-01T00:00:00Z\"]]],"
                                    "\"1970-01-01T00:00:00Z\"]]],"
                                 "\"1970-01-01T00:00:00Z\"]"));

    // Test that appending it back to a row works
    
    std::vector<std::tuple<Path, CellValue, Date> > vals2;
    Path prefix = PathElement("out");
    val.appendToRowDestructive(prefix, vals2);

    std::string expected
        = "["
	"[ \"out.a.a\", 1, \"1970-01-01T00:00:00Z\" ],"
	"[ \"out.a.b\", 3, \"1970-01-01T00:00:00Z\" ],"
	"[ \"out.b.a\", 2, \"1970-01-01T00:00:00Z\" ],"
	"[ \"out.b.b\", 4, \"1970-01-01T00:00:00Z\" ] ]";
    BOOST_CHECK_EQUAL(jsonEncode(vals2), Json::parse(expected));
}

BOOST_AUTO_TEST_CASE(test_get_nested_column)
{
    std::vector<std::tuple<Path, CellValue, Date> > vals;
    Date ts;
    vals.emplace_back(PathElement("a"), 1, ts);
    vals.emplace_back(PathElement("b"), 2, ts);
    vals.emplace_back(PathElement("c") + PathElement("a"), 3, ts);
    vals.emplace_back(PathElement("c") + PathElement("b"), 4, ts);
    vals.emplace_back(PathElement("d"), 5, ts);

    ExpressionValue val(vals);

    // Test that we can get a nested field
    Path path0;
    Path path1 = PathElement("a");
    Path path2 = PathElement("c");
    Path path3 = PathElement("c") + PathElement("a");
    Path path4 = PathElement("c") + PathElement("a") + PathElement("x");
    
    ExpressionValue val0 = val.getNestedColumn(path0);
    ExpressionValue val1 = val.getNestedColumn(path1);
    ExpressionValue val2 = val.getNestedColumn(path2);
    ExpressionValue val3 = val.getNestedColumn(path3);
    ExpressionValue val4 = val.getNestedColumn(path4);

    BOOST_CHECK_EQUAL(val0.extractJson(), val.extractJson());
    BOOST_CHECK_EQUAL(val1.getAtom(), 1);
    BOOST_CHECK_EQUAL(val2.extractJson(), Json::parse("{\"a\":3,\"b\":4}"));
    BOOST_CHECK_EQUAL(val3.getAtom(), 3);
    BOOST_CHECK(val4.empty());
}

BOOST_AUTO_TEST_CASE(test_embedding_get_column)
{
    vector<double> vals = { 1, 2, 3, 4.5, 6, 12 };
    Date ts;

    // Create a 1x6 vector
    ExpressionValue val(vals, ts);

    BOOST_CHECK_EQUAL(val.getNestedColumn({0}).getAtom(), 1);
    BOOST_CHECK_EQUAL(val.getNestedColumn({2}).getAtom(), 3);
    BOOST_CHECK_EQUAL(val.getNestedColumn({4}).getAtom(), 6);
    BOOST_CHECK(val.getNestedColumn({0,0}).empty());
    BOOST_CHECK_EQUAL(val.getNestedColumn({10221312213021}).getAtom(), CellValue());
    BOOST_CHECK_EQUAL(val.getNestedColumn({-10221312213021}).getAtom(), CellValue());
    BOOST_CHECK_EQUAL(val.getNestedColumn({-1}).getAtom(), CellValue());
    BOOST_CHECK_EQUAL(val.getNestedColumn({"hello"}).getAtom(), CellValue());

    // Create a 2x3 matrix
    ExpressionValue val2(vals, ts, {2,3});

    // Test in bounds 2 dimensional accesses
    BOOST_CHECK_EQUAL(val2.getNestedColumn({0,0}).getAtom(), 1);
    BOOST_CHECK_EQUAL(val2.getNestedColumn({0,2}).getAtom(), 3);
    BOOST_CHECK(val2.getNestedColumn({0,4}).empty());
    BOOST_CHECK_EQUAL(val2.getNestedColumn({1,0}).getAtom(), 4.5);
    BOOST_CHECK_EQUAL(val2.getNestedColumn({1,2}).getAtom(), 12);
    BOOST_CHECK(val2.getNestedColumn({1,4}).empty());

    // Test out of bound accesses
    BOOST_CHECK(val2.getNestedColumn({10221312213021}).empty());
    BOOST_CHECK(val2.getNestedColumn({-10221312213021}).empty());
    BOOST_CHECK(val2.getNestedColumn({-1}).empty());
    BOOST_CHECK(val2.getNestedColumn({"hello"}).empty());

}

BOOST_AUTO_TEST_CASE( test_unflatten_nested_row_bad_order )
{
    StructValue sub1, sub2;
    Date ts;
    PathElement a("a"), b("b");
    sub1.emplace_back(a, ExpressionValue(1, ts));
    sub1.emplace_back(b, ExpressionValue(2, ts));
    sub2.emplace_back(a, ExpressionValue(3, ts));
    sub2.emplace_back(b, ExpressionValue(4, ts));

    StructValue valInit;
    valInit.emplace_back(a, sub1);
    valInit.emplace_back(b, sub2);

    ExpressionValue val(valInit);

    BOOST_CHECK_EQUAL(val.getColumn(a).extractJson(),
                      Json::parse("{'a': 1, 'b': 2}"));
    BOOST_CHECK_EQUAL(val.getColumn(b).extractJson(),
                      Json::parse("{'a': 3, 'b': 4}"));

    BOOST_CHECK_EQUAL(val.getNestedColumn(a).extractJson(),
                      Json::parse("{'a': 1, 'b': 2}"));
    BOOST_CHECK_EQUAL(val.getNestedColumn(b).extractJson(),
                      Json::parse("{'a': 3, 'b': 4}"));

    BOOST_CHECK_EQUAL(val.getNestedColumn(a + a).extractJson(), 1);
    BOOST_CHECK_EQUAL(val.getNestedColumn(a + b).extractJson(), 2);
    BOOST_CHECK_EQUAL(val.getNestedColumn(b + a).extractJson(), 3);
    BOOST_CHECK_EQUAL(val.getNestedColumn(b + b).extractJson(), 4);
}

BOOST_AUTO_TEST_CASE( test_unflatten_nested_row_doubled )
{
    StructValue sub1, sub2, sub3, sub4;
    Date ts;
    PathElement a("a"), b("b");
    sub1.emplace_back(a, ExpressionValue(1, ts));
    sub2.emplace_back(b, ExpressionValue(2, ts));
    sub3.emplace_back(a, ExpressionValue(3, ts));
    sub4.emplace_back(b, ExpressionValue(4, ts));

    StructValue valInit;
    valInit.emplace_back(a, sub1);
    valInit.emplace_back(b, sub3);
    valInit.emplace_back(a, sub2);
    valInit.emplace_back(b, sub4);

    ExpressionValue val(valInit);

    BOOST_CHECK_EQUAL(val.getColumn(a).extractJson(),
                      Json::parse("{'a': 1, 'b': 2}"));
    BOOST_CHECK_EQUAL(val.getColumn(b).extractJson(),
                      Json::parse("{'a': 3, 'b': 4}"));

    BOOST_CHECK_EQUAL(val.getNestedColumn(a).extractJson(),
                      Json::parse("{'a': 1, 'b': 2}"));
    BOOST_CHECK_EQUAL(val.getNestedColumn(b).extractJson(),
                      Json::parse("{'a': 3, 'b': 4}"));

    BOOST_CHECK_EQUAL(val.getNestedColumn(a + a).extractJson(), 1);
    BOOST_CHECK_EQUAL(val.getNestedColumn(a + b).extractJson(), 2);
    BOOST_CHECK_EQUAL(val.getNestedColumn(b + a).extractJson(), 3);
    BOOST_CHECK_EQUAL(val.getNestedColumn(b + b).extractJson(), 4);
}

BOOST_AUTO_TEST_CASE( test_deeply_nested )
{
    PathElement f("f"), r("r"), a("a"), b("b"), c("c"), d("d");
    
    Date ts;

    StructValue sub1, sub2, sub3, sub4;
    sub1.emplace_back(a, ExpressionValue(1, ts));
    sub2.emplace_back(b, ExpressionValue(2, ts));
    sub3.emplace_back(c, ExpressionValue(3, ts));
    sub4.emplace_back(d, ExpressionValue(4, ts));
    
    StructValue nest1, nest2, nest3, nest4;
    nest1.emplace_back(r, sub1);
    nest2.emplace_back(r, sub2);
    nest3.emplace_back(r, sub3);
    nest4.emplace_back(r, sub4);

    StructValue inner;
    inner.emplace_back(r, sub1);
    inner.emplace_back(r, sub2);
    inner.emplace_back(r, sub3);
    inner.emplace_back(r, sub4);

    ExpressionValue innerv(inner);

    BOOST_CHECK_EQUAL(innerv.extractJson(),
                      Json::parse("{ 'r': {'a' : 1,'b' : 2,'c' : 3,'d' : 4}}"));

    BOOST_CHECK_EQUAL(innerv.getColumn(r).extractJson(),
                      Json::parse("{'a' : 1,'b' : 2,'c' : 3,'d' : 4}"));

    BOOST_CHECK_EQUAL(innerv.getNestedColumn(r + a).extractJson(),
                      Json::parse("1"));
    BOOST_CHECK_EQUAL(innerv.getNestedColumn(r + b).extractJson(),
                      Json::parse("2"));
    BOOST_CHECK_EQUAL(innerv.getNestedColumn(r + c).extractJson(),
                      Json::parse("3"));
    BOOST_CHECK_EQUAL(innerv.getNestedColumn(r + d).extractJson(),
                      Json::parse("4"));

    auto onColumn = [&] (PathElement coord,
                         ExpressionValue val)
        {
            cerr << "got " << coord << " = " << jsonEncode(val) << endl;
            return true;
        };

    innerv.forEachColumn(onColumn);

#if 0
    StructValue outer;
    outer.emplace_back(r, nest1);
    outer.emplace_back(r, nest2);
    outer.emplace_back(r, nest3);
    outer.emplace_back(r, nest4);
#endif
}

BOOST_AUTO_TEST_CASE( test_get_filtered_superposition )
{
    Date ts1, ts2 = ts1.plusSeconds(1), ts3 = ts2.plusSeconds(1);

    auto makeVal = [&] () -> ExpressionValue
        {
            StructValue result;
            result.emplace_back(PathElement(), ExpressionValue(1, ts1));
            result.emplace_back(PathElement(), ExpressionValue(2, ts2));
            result.emplace_back(PathElement(), ExpressionValue(3, ts3));
            return result;
        };

    auto makeAndFilter = [&] (VariableFilter filter) -> ExpressionValue
        {
            ExpressionValue val = makeVal();
            ExpressionValue storage;
            ExpressionValue result = val.getFiltered(filter, storage);
            cerr << "returning " << jsonEncode(result) << endl;
            return result;
        };

    BOOST_CHECK_EQUAL(makeAndFilter(GET_LATEST).getAtom(),
                      3);
    BOOST_CHECK_EQUAL(makeAndFilter(GET_EARLIEST).getAtom(),
                      1);
    BOOST_CHECK_EQUAL(makeAndFilter(GET_ANY_ONE).getAtom(),
                      1);
    string expected =
        "[[[null,[ 1, '1970-01-01T00:00:00Z' ]],"
        "  [null,[ 2, '1970-01-01T00:00:01Z' ]],"
	"  [null,[ 3, '1970-01-01T00:00:02Z' ]]],"
        "'1970-01-01T00:00:02Z']";

    BOOST_CHECK_EQUAL(jsonEncode(makeAndFilter(GET_ALL)),
                      Json::parse(expected));

    auto makeAndFilterDestructive = [&] (VariableFilter filter) -> ExpressionValue
        {
            ExpressionValue val = makeVal();
            return val.getFilteredDestructive(filter);
        };

    BOOST_CHECK_EQUAL(makeAndFilterDestructive(GET_LATEST).getAtom(),
                      3);
    BOOST_CHECK_EQUAL(makeAndFilterDestructive(GET_EARLIEST).getAtom(),
                      1);
    BOOST_CHECK_EQUAL(makeAndFilterDestructive(GET_ANY_ONE).getAtom(),
                      1);
    BOOST_CHECK_EQUAL(jsonEncode(makeAndFilterDestructive(GET_ALL)),
                      Json::parse(expected));
}

BOOST_AUTO_TEST_CASE( test_get_filtered_nested )
{
    Date ts1, ts2 = ts1.plusSeconds(1), ts3 = ts2.plusSeconds(1);

    auto makeVal = [&] () -> ExpressionValue
        {
            StructValue result;
            result.emplace_back(PathElement(), ExpressionValue(1, ts1));
            result.emplace_back(PathElement(), ExpressionValue(2, ts2));
            result.emplace_back(PathElement(), ExpressionValue(3, ts3));

            StructValue outer;
            outer.emplace_back(PathElement("a"), std::move(result));
            return outer;
        };

    auto makeAndFilter = [&] (VariableFilter filter) -> ExpressionValue
        {
            ExpressionValue val = makeVal();
            ExpressionValue storage;
            ExpressionValue result = val.getFiltered(filter, storage);
            cerr << "returning " << jsonEncode(result) << endl;
            return result;
        };

    BOOST_CHECK_EQUAL(makeAndFilter(GET_LATEST).extractJson().toStringNoNewLine(),
                      "{\"a\":3}");
    BOOST_CHECK_EQUAL(makeAndFilter(GET_EARLIEST).extractJson().toStringNoNewLine(),
                      "{\"a\":1}");
    BOOST_CHECK_EQUAL(makeAndFilter(GET_ANY_ONE).extractJson().toStringNoNewLine(),
                      "{\"a\":1}");

    string expected =
        "[[['a', [[[null,[ 1, '1970-01-01T00:00:00Z' ]],"
        "          [null,[ 2, '1970-01-01T00:00:01Z' ]],"
	"          [null,[ 3, '1970-01-01T00:00:02Z' ]]],"
        "        '1970-01-01T00:00:02Z']]],"
        " '1970-01-01T00:00:02Z']";

    BOOST_CHECK_EQUAL(jsonEncode(makeAndFilter(GET_ALL)),
                      Json::parse(expected));

    auto makeAndFilterDestructive = [&] (VariableFilter filter) -> ExpressionValue
        {
            ExpressionValue val = makeVal();
            return val.getFilteredDestructive(filter);
        };

    BOOST_CHECK_EQUAL(makeAndFilterDestructive(GET_LATEST).extractJson().toStringNoNewLine(),
                      "{\"a\":3}");
    BOOST_CHECK_EQUAL(makeAndFilterDestructive(GET_EARLIEST).extractJson().toStringNoNewLine(),
                      "{\"a\":1}");
    BOOST_CHECK_EQUAL(makeAndFilterDestructive(GET_ANY_ONE).extractJson().toStringNoNewLine(),
                      "{\"a\":1}");
    BOOST_CHECK_EQUAL(jsonEncode(makeAndFilterDestructive(GET_ALL)),
                      Json::parse(expected));
}

BOOST_AUTO_TEST_CASE( test_get_embedding_atom )
{
    CellValue atom("hello");
    Date ts;

    ExpressionValue val(atom, ts);

    {
        /// Test that extracting from an atom throws as it's not an embedding
        MLDB_TRACE_EXCEPTIONS(false);
        BOOST_CHECK_THROW(val.getEmbedding(nullptr, 0), HttpReturnException);
    }
}

BOOST_AUTO_TEST_CASE( test_superposition_with_search_row )
{
    Date ts;

    RowValue row;
    row.emplace_back(PathElement("a"), 1, ts);
    row.emplace_back(PathElement("a"), 2, ts.plusSeconds(1));

    string expected =
        "[[[ 'a', [[[null,[ 1, '1970-01-01T00:00:00Z']], "
        "         [null, [ 2, '1970-01-01T00:00:01Z' ]]], "
        "         '1970-01-01T00:00:01Z']]], "
        "'1970-01-01T00:00:01Z']";

    ExpressionValue val(row);

    BOOST_CHECK(!val.isSuperposition() && val.isRow() && !val.isAtom());
    BOOST_CHECK_EQUAL(jsonEncode(val), Json::parse(expected));

    ExpressionValue found;

    searchRow(row, PathElement("a"), GET_ALL, found);
    BOOST_CHECK(found.isSuperposition() && found.isRow() && !found.isAtom());
    expected = " [[[null,[ 1, '1970-01-01T00:00:00Z']], "
        "         [null, [ 2, '1970-01-01T00:00:01Z' ]]], "
        "         '1970-01-01T00:00:01Z']]";
    BOOST_CHECK_EQUAL(jsonEncode(found), Json::parse(expected));

    searchRow(row, PathElement("a"), GET_LATEST, found);
    BOOST_CHECK(!found.isSuperposition() && !found.isRow() && found.isAtom());
    expected = "[ 2, '1970-01-01T00:00:01Z' ]";
    BOOST_CHECK_EQUAL(jsonEncode(found), Json::parse(expected));

    searchRow(row, PathElement("a"), GET_EARLIEST, found);
    BOOST_CHECK(!found.isSuperposition() && !found.isRow() && found.isAtom());
    expected = "[ 1, '1970-01-01T00:00:00Z' ]";
    BOOST_CHECK_EQUAL(jsonEncode(found), Json::parse(expected));
}

BOOST_AUTO_TEST_CASE( test_embedding_length )
{
    std::vector<float> values = {1,2,3,4};
    Date ts;
    DimsVector shape = {2,2};

    ExpressionValue myValue(values,
                            ts,
                            shape);

    BOOST_CHECK_EQUAL(myValue.rowLength(), 2);
    BOOST_CHECK_EQUAL(myValue.getAtomCount(), 4);
}
