// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/** behavior_utils.cc                                             -*- C++ -*-
    Rémi Attab, 28 Apr 2015
    Copyright (c) 2015 mldb.ai inc.  All rights reserved.
*/

#include "behavior_utils.h"

#include "mldb/plugins/behavior/mutable_behavior_domain.h"
#include "mldb/base/parse_context.h"
#include "mldb/types/json_parsing.h"
#include "mldb/types/basic_value_descriptions.h"
#include "mldb/sql/path.h"
#include <algorithm>


using namespace std;



namespace MLDB {
namespace behaviors {


/******************************************************************************/
/* ENCODE/ DECODE COLUMN                                                      */
/******************************************************************************/

// Column encoding was designed to be compatible with legacy behavior
// files to allow for MLDB to be adopted more quickly.  The main requirements
// are:
// 1.  Legacy behavior files, which did no validation on their behavior
//     name, should be able to be loaded, even if they have invalid UTF-8
//     characters.  Note that most of these had no values at all, just keys.
// 2.  Behavior files produced by MLDB should be readable by MLDB with no
//     loss of information or ambiguity.
//
// It is acceptable to use exceptions when parsing legacy behavior files to
// do a best-effort job of understanding what was meant, as each behavior
// is only processed once when loading the file and there are at most a couple
// of million behaviors.

namespace {

std::pair<std::string, std::string>
splitColumn(const Id & id)
{
    std::string idStr = id.toString();

    // Legacy systems did not encode the = sign in any way, and so it's
    // impossible to know if an = was in the key or the value.  Here we
    // assume that it's in the value.
    auto pos = idStr.find('=');
    if (pos == std::string::npos)
        return std::make_pair(std::move(idStr), "");
    
    return std::make_pair(idStr.substr(0, pos), idStr.substr(pos + 1));
}

ColumnPath
decodeLegacyColumnPath(std::string key)
{
    // Legacy behavior files from certain systems added a "0:" prefix to
    // everything, which we remove here.  This should be disabled for
    // non-legacy systems.
    if (key.find("0:") == 0)
        key = std::string(key, 2);

    ExcCheck(!key.empty(), "empty column key");

    return ColumnPath(std::move(key));
}

// Decode the value of a value from a legacy behavior file.
// This is a best-effort system, as those behavior files did
// not do a good job of consistently encoding.  We first try
// to parse it as a JSON cell value.  If that doesn't work, we
// try to infer a cell value.  If that doesn't work (which is
// usually because of an UTF-8 encoding issue) we convert to
// ASCII and use that... at least we can load the behavior
// file.  Since this is only used once when loading the file,
// and only applies once per behavior (usually 10,000 to
// 100,000 times), it executes in a fraction of a second even
// using exceptions and so is not worth optimizing.
//
// Note that most legacy behavior files don't actually have
// values in their columns.  If this is the case, we happily
// return a null value.
CellValue
decodeLegacyColumnValue(const std::string & value)
{
    if (value.empty())
        return CellValue();
    try {
        MLDB_TRACE_EXCEPTIONS(false);
        return jsonDecodeStr<CellValue>(value);
    }
    catch (const std::exception & exc) {}

    try {
        MLDB_TRACE_EXCEPTIONS(false);
        return CellValue::parse(value);
    }
    catch (const std::exception & exc) {}

    std::string asAscii;
    for (int i = 0; i < value.length();  ++i) {
        unsigned char c = value[i];
        if (c < ' ' || c >= 127)
            asAscii += MLDB::format("%%%02x", c);
        else asAscii += c;
    }

    return CellValue(std::move(asAscii));
}

} // namespace anonymous


ColumnPath
decodeColumnPath(const Id & id, bool couldBeLegacy)
{
    static auto cellValueDescription = getDefaultDescriptionSharedT<CellValue>();

    std::string s = id.toString();
    if (!couldBeLegacy || (s.size() > 0 && s[0] == '"')) {
        StreamingJsonParsingContext context(s, s.c_str(), s.length());
        Utf8String key = context.expectStringUtf8();
        context.context->expect_literal('=');
        return ColumnPath(key);
    }
    
    auto split = splitColumn(id);
    return decodeLegacyColumnPath(std::move(split.first));
}

std::pair<ColumnPath, CellValue>
decodeColumn(const Id & id, bool couldBeLegacy)
{
    static auto cellValueDescription = getDefaultDescriptionSharedT<CellValue>();

    std::string s = id.toString();
    
    if (!couldBeLegacy || (s.size() > 0 && s[0] == '"')) {
        StreamingJsonParsingContext context(s, s.c_str(), s.length());
        Utf8String key = context.expectStringUtf8();
        context.context->expect_literal('=');
        CellValue value;
        cellValueDescription->parseJsonTyped(&value, context);
        context.context->expect_eof();

        return { ColumnPath(key), value };
    }
    
    auto split = splitColumn(id);
    return std::make_pair(
            decodeLegacyColumnPath(std::move(split.first)),
            decodeLegacyColumnValue(std::move(split.second)));
}

Id
encodeColumn(const ColumnPath& name, const CellValue& value)
{
    return Id(jsonEncodeStr(name.toUtf8String()) + "=" + jsonEncodeStr(value));
}

PathElement 
toPathElement(const Id & id)
{
    if (id.type == Id::SHORTSTR) {
        return PathElement(id.shortStr, strnlen(id.shortStr, 16));
    }
    else {
        //some legacy data contains null chars in the string, lets skip the check for now.
        PathElement coord;
        coord.initStringUnchecked(id.toUtf8String());
        return coord;
    }
}

bool isInteger(const char * bytes, size_t len) {
    // this condition is mimicking the condition in Id::parse
    return ((std::find_if_not(bytes, bytes + len, ::isdigit) == bytes + len)
            && len != 0 && len < 40
            && bytes[0] != '0') 
        || (len == 1 && bytes[0] == '0');
}

bool isNull(const char * bytes, size_t len) {
    return (len == 4 && strcmp(bytes, "null") == 0);
}

Id
toId(const PathElement & coord)
{
    // Id id(coord.data(), coord.dataLength());
    if (isInteger(coord.data(), coord.dataLength())) {
        // ExcAssertEqual(id.type, Id::BIGDEC);
        return Id(coord.data(), coord.dataLength(), Id::BIGDEC);
    }
    else if (isNull(coord.data(), coord.dataLength())) {
        // ExcAssertEqual(id.type, Id::NULLID);
        return Id(coord.data(), coord.dataLength(), Id::NULLID);
    }
    else if (!coord.complex_ && coord.dataLength() < 16) {  // length 16 might be BASE64_96 - handled below
        // ExcAssertEqual(id.type, Id::SHORTSTR);
        return Id(coord.data(), coord.dataLength(), Id::SHORTSTR);
    }
    else {
        return Id(coord.data(), coord.dataLength());
    }
}

Id
toId(const Path & coord)
{
    ExcAssert(coord.size() == 1); //TODO: review
    return toId(coord[0]);
}

} // namespace behaviors
} // namespace MLDB

