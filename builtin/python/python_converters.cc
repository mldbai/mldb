/** python_converters.cc
    Jeremy Barnes, 7 December 2015
    Copyright (c) 2015 mldb.ai inc.  All rights reserved.

    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
*/

#include "python_converters.h"
#include "python_interpreter.h"
#include "from_python_converter.h"
#include "mldb_python_converters.h"
#include "Python.h"
#include "datetime.h"
#include <iostream>
#include "nanobind/stl/string.h"
#include "nanobind/stl/vector.h"
#include "nanobind/stl/pair.h"
#include "mldb/types/json_visitor.h"
#include "mldb/sql/cell_value_visitor.h"
#include "python_visitor.h"

using namespace std;

namespace MLDB {
namespace Python {


/******************************************************************************/
/* Json::Value CONVERTER                                                      */
/******************************************************************************/

Utf8String srepr(nanobind::handle obj)
{
    Utf8String result;
    nanobind::str type_repr(nanobind::steal(PyObject_Repr(obj.type().ptr())));
    result = type_repr.c_str() + string(" = ");
    nanobind::str rep_obj(nanobind::steal(PyObject_Repr(obj.ptr())));
    result += rep_obj.c_str();
    return result;
}

static Json::Value
json_from_python_recursive(nanobind::handle src, uint8_t flags, nanobind::detail::cleanup_list *cleanup)
{
    //cerr << "converting " << srepr(src) << " to Json" << endl;

    auto handle_list = [&] (auto list) -> Json::Value
    {
        Json::Value val(Json::ValueType::arrayValue);
        for (auto obj: list)
            val.append(json_from_python_recursive(obj, flags, cleanup));
        return val;
    };

    auto handle_dict = [&] (nanobind::dict d) -> Json::Value
    {
        Json::Value val(Json::objectValue);
        for (auto && [key, value]: d)
            val[nanobind::cast<Utf8String>(key)] = json_from_python_recursive(value, flags, cleanup);
        return val;
    };

    PyObjectLambdaVisitor visitor {
        ExceptionOnUnknownPyObjectReturning<Json::Value>(),
        [&] (PythonNoneTag)           { return Json::Value(); },
        [&] (nanobind::bool_ b)       { return Json::Value(nanobind::cast<bool>(b)); },
        [&] (nanobind::int_ i)        { return Json::Value(PyLong_AS_LONG(i.ptr())); /*Json::Value(nanobind::cast<unsigned long long>(i));*/ },
        [&] (nanobind::float_ f)      { return Json::Value(nanobind::cast<double>(f)); },
        [&] (nanobind::str s)         { return Json::Value(Utf8String(s.c_str())); },
        [&] (nanobind::tuple t)       { return handle_list(t); },
        [&] (nanobind::list l)        { return handle_list(l); },
        [&] (nanobind::dict d)        { return handle_dict(d); },
    };

    try {
        return visit(visitor, src);
    } catch (...) {
        cerr << "json from python returned exception " << getExceptionString() << " on value " << srepr(src) << endl;
        throw;
    }
}

bool from_python(nanobind::handle src, uint8_t flags, nanobind::detail::cleanup_list *cleanup, Json::Value * result) noexcept
{
    try {
        *result = json_from_python_recursive(src, flags, cleanup);
        return true;
    } catch (const std::exception & e) {
        return false;
    } MLDB_CATCH_ALL {
        return false;
    }
}

nanobind::object json_from_cpp_recursive(StructuredJsonParsingContext & context, nanobind::rv_policy policy, nanobind::detail::cleanup_list *cleanup)
{
    JsonLambdaVisitor visitor {
        ExceptionOnUnknownJsonReturning<nanobind::object>(context),
        [&] (JsonNullTag)          { return nanobind::none(); },
        [&] (bool b)               { return nanobind::bool_(b); },
        [&] (int64_t i)            { return nanobind::int_(i); },
        [&] (uint64_t i)           { return nanobind::int_(i); },
        [&] (double d)             { return nanobind::float_(d); },
        [&] (const Utf8String & s) { return nanobind::str(s.rawData(), s.rawLength()); },
        [&] (JsonObjectTag)
        {
            nanobind::dict dict;
            context.forEachMember([&] () { dict[context.fieldName().c_str()] = json_from_cpp_recursive(context, policy, cleanup); });
            return dict;
        },
        [&] (JsonArrayTag)
        {
            nanobind::list lst;
            context.forEachElement([&] () { lst.append(json_from_cpp_recursive(context, policy, cleanup)); });
            return lst;
        }
    };

    return visit(visitor, context);
}

nanobind::handle from_cpp(const Json::Value & value, nanobind::rv_policy policy, nanobind::detail::cleanup_list *cleanup)
{
    StructuredJsonParsingContext context(value);
    return json_from_cpp_recursive(context, policy, cleanup).release();
}

bool from_python(nanobind::handle src, uint8_t flags, nanobind::detail::cleanup_list *cleanup, Utf8String * result) noexcept
{
    try {
        // TODO: don't copy the string if it's long
        *result = Utf8String(nanobind::str(src).c_str(), false /* check valid UTF-8 */);
        return true;
    } catch (const std::exception & e) {
        return false;
    } MLDB_CATCH_ALL {
        return false;
    }
}

nanobind::handle from_cpp(const Utf8String & value, nanobind::rv_policy policy, nanobind::detail::cleanup_list *cleanup)
{
    return nanobind::str(value.c_str()).release();
}

bool from_python(nanobind::handle src, uint8_t flags, nanobind::detail::cleanup_list *cleanup, Date * result) noexcept
{
    //cerr << "Date from_python " << srepr(src) << endl;

    bool found = true;
    PyObjectLambdaVisitor visitor {
        [&] (nanobind::handle unk)    { found = false; }, // default case
        [&] (nanobind::int_ i)        { *result = Date::fromSecondsSinceEpoch(nanobind::cast<long>(src)); },
        [&] (nanobind::float_ f)      { *result = Date::fromSecondsSinceEpoch(nanobind::cast<double>(src)); },
        [&] (nanobind::str s)         { *result = Date::parseIso8601DateTime(s.c_str()); },
        [&] (Date d)                  { *result = d; },
    };

    try {
        visit(visitor, src);
    } MLDB_CATCH_ALL {
        found = false;
    }
    return found;
}

nanobind::handle from_cpp(const Date & value, nanobind::rv_policy policy, nanobind::detail::cleanup_list *cleanup)
{
    return PyDateTime_FromDateAndTime(value.year(), value.monthOfYear(), value.dayOfMonth(),
                                      value.hour(), value.minute(), value.second(), value.microsecond());
}

bool from_python(nanobind::handle src, uint8_t flags, nanobind::detail::cleanup_list *cleanup, CellValue * result) noexcept
{
    bool found = true;
    PyObjectLambdaVisitor visitor {
        [&] (nanobind::handle unk)    { found = false; }, // default case
        [&] (PythonNoneTag)           { *result = CellValue(); },
        [&] (nanobind::bool_ b)       { *result = CellValue((bool)b); },
        [&] (nanobind::int_ i)        { *result = CellValue((long long)i); }, // TODO: out of range integers
        [&] (nanobind::float_ f)      { *result = (double)f; },
        [&] (nanobind::str s)         { *result = Utf8String(s.c_str()); },
        [&] (Date d)                  { *result = d; },
    };

    try {
        visit(visitor, src);
    } MLDB_CATCH_ALL {
        found = false;
    }
    return found;
}

nanobind::handle from_cpp(const CellValue & value, nanobind::rv_policy policy, nanobind::detail::cleanup_list *cleanup)
{
    CellValueLambdaVisitor visitor {
        ExceptionOnUnknownCellValueReturning<nanobind::object>(),
        [&] (CellValueEmptyTag)    { return nanobind::none(); },
        [&] (int64_t i)            { return nanobind::int_(i); },
        [&] (uint64_t i)           { return nanobind::int_(i); },
        [&] (double d)             { return nanobind::float_(d); },
        [&] (const Utf8String & s) { return nanobind::str(s.rawData(), s.rawLength()); },
        // ...
    };

    return visit(visitor, value);
}

bool from_python(nanobind::handle src, uint8_t flags, nanobind::detail::cleanup_list *cleanup, Path * result) noexcept
{
    auto handle_list = [&] (auto list)
    {
        PathBuilder builder;
        for (auto && el: list)
            builder.add(nanobind::cast<PathElement>(el));
        return builder.extract();
    };

    bool found = true;
    PyObjectLambdaVisitor visitor {
        [&] (nanobind::handle unk)    { found = false; }, // default case
        [&] (PythonNoneTag)           { *result = Path(); },
        [&] (nanobind::int_ i)        { *result = PathElement(nanobind::cast<unsigned long>(i)); },
        [&] (nanobind::str s)         { *result = PathElement(s.c_str()); },
        [&] (nanobind::list l)        { *result = handle_list(l); },
        [&] (nanobind::tuple t)       { *result = handle_list(t); },
    };

    try {
        visit(visitor, src);
    } MLDB_CATCH_ALL {
        found = false;
    }
    return found;
}

nanobind::handle from_cpp(const Path & value, nanobind::rv_policy policy, nanobind::detail::cleanup_list *cleanup)
{
    return nanobind::str(value.toUtf8String().c_str()).release();
}

bool from_python(nanobind::handle src, uint8_t flags, nanobind::detail::cleanup_list *cleanup, PathElement * result) noexcept
{
    bool found = true;
    PyObjectLambdaVisitor visitor {
        [&] (nanobind::handle unk)    { found = false; }, // default case
        [&] (PythonNoneTag)           { *result = PathElement(); },
        [&] (nanobind::int_ i)        { *result = PathElement(nanobind::cast<unsigned long>(i)); },
        [&] (nanobind::str s)         { *result = PathElement(s.c_str()); },
    };

    try {
        visit(visitor, src);
    } MLDB_CATCH_ALL {
        found = false;
    }
    return found;
}

nanobind::handle from_cpp(const PathElement & value, nanobind::rv_policy policy, nanobind::detail::cleanup_list *cleanup)
{
    cerr << "TODO: implement from_cpp for PathElement" << endl;
    MLDB_THROW_UNIMPLEMENTED();
}

// We need to ensure that anything that's a value is serialized as Json, not as a Python object
// to pass to RestParams. This code ensures that this happens.
bool from_python(nanobind::handle src, uint8_t flags, nanobind::detail::cleanup_list *cleanup, RestParams * result) noexcept
{
    auto handle_key = [] (nanobind::object key) -> Utf8String
    {
        return nanobind::str(key).c_str();
    };

    auto handle_value = [] (nanobind::object value) -> Utf8String
    {
        // Strings stay as strings; everything else gets converted to Json and stringified
        PyObjectLambdaVisitor visitor {
            [&] (nanobind::handle unk) -> Utf8String // default case
            {
                auto j = nanobind::cast<Json::Value>(unk);
                return j.toStringUtf8();
            },
            [&] (nanobind::str s)         { return nanobind::cast<Utf8String>(s); },
        };

        return visit(visitor, value);
    };

    auto handle_pair = [&] (nanobind::object pair) -> std::pair<Utf8String, Utf8String>
    {
        nanobind::tuple tpl(pair);
        if (nanobind::len(tpl) != 2)
            throw MLDB::Exception("expected key-value pairs for RestParams");
        return { handle_key(tpl[0]), handle_value(tpl[1]) };
    };

    auto handle_list = [&] (nanobind::object lst) -> RestParams
    {
        RestParams result;
        auto len = nanobind::len(lst);
        for (size_t i = 0; i < len; ++i) {
            result.push_back(handle_pair(lst[i]));
        }
        return result;
    };

    auto handle_dict = [&] (nanobind::dict d) -> RestParams
    {
        RestParams result;
        for (auto && [key, value]: d) {
            result.emplace_back(handle_key(nanobind::borrow(key)), handle_value(nanobind::borrow(value)));
        }
        return result;
    };

    bool found = true;
    PyObjectLambdaVisitor visitor {
        [&] (nanobind::handle unk)    { found = false; }, // default case
        [&] (PythonNoneTag)           { *result = RestParams(); },
        [&] (nanobind::list l)        { *result = handle_list(l); },
        [&] (nanobind::tuple l)       { *result = handle_list(l); },
        [&] (nanobind::dict d)        { *result = handle_dict(d); },
    };

    try {
        visit(visitor, src);
        //cerr << "RestParams: converted " << srepr(src) << " to" << endl;
        //for (auto && [key, value]: *result) {
        //    cerr << key << "=" << value << endl;
        //}
    } MLDB_CATCH_ALL {
        //cerr << "RestParams: converting " << srepr(src) << " resulted in exception " << MLDB::getExceptionString() << endl;
        found = false;
    }
    return found;
}

nanobind::handle from_cpp(const RestParams & value, nanobind::rv_policy policy, nanobind::detail::cleanup_list *cleanup)
{
    return nanobind::detail::make_caster<RestParamsBase>::from_cpp(value, policy, cleanup);
}

void pythonConvertersInit(const EnterThreadToken & thread)
{
    PyDateTime_IMPORT;
}

RegisterPythonInitializer regMe(&pythonConvertersInit);

} // namespace Python
} // namespace MLDB
