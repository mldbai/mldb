// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* json_diff.h                                                     -*- C++ -*-
   Jeremy Barnes, 229 October 2013
   Copyright (c) 2013 mldb.ai inc.  All rights reserved.

   Diff for JSON objects.
*/

#pragma once


#include <map>
#include <string>
#include "mldb/ext/jsoncpp/json.h"
#include "mldb/jml/utils/unnamed_bool.h"
#include "mldb/types/value_description_fwd.h"

namespace MLDB {

struct JsonArrayElementDiff;


/*****************************************************************************/
/* JSON DIFF                                                                 */
/*****************************************************************************/

struct JsonDiff {
    struct Deleted {
    };

    static Deleted deleted;

    JsonDiff()
    {
    }

    JsonDiff(const Json::Value & oldValue,
             const Json::Value & newValue)
        : oldValue(new Json::Value(oldValue)),
          newValue(new Json::Value(newValue))
    {
    }

    JsonDiff(const Json::Value & oldValue,
             const Deleted &)
        : oldValue(new Json::Value(oldValue))
    {
    }

    JsonDiff(const Deleted &,
             const Json::Value & newValue)
        : newValue(new Json::Value(newValue))
    {
    }

    bool empty() const
    {
        return fields.empty()
            && elements.empty()
            && !oldValue
            && !newValue;
    }

    /** Ignore the given values.  The regex will be called with strings like this:
        
        key1.key2.value:+new,-old

        Any which the regex matches against that string will be removed from the
        diff.
    */
    void ignore(const std::string & toIgnoreRegex, bool debug = false);

    /** Functional version of ignore(). */
    JsonDiff ignored(const std::string & toIgnoreRegex, bool debug = false) const;

    MLDB_IMPLEMENT_OPERATOR_BOOL(!empty());
 
    // Reverse the patch
    void reverse();
   
    // For an object
    std::map<std::string, JsonDiff> fields;

    // For an array.
    std::vector<JsonArrayElementDiff> elements;

    // For a straight out diff.  Null pointer means deleted
    std::shared_ptr<Json::Value> oldValue;
    std::shared_ptr<Json::Value> newValue;
};

std::ostream & operator << (std::ostream & stream, const JsonDiff & diff);

struct JsonArrayElementDiff: public JsonDiff {
    JsonArrayElementDiff()
        : oldEl(-2), newEl(-2)
    {
    }

    JsonArrayElementDiff(int oldEl, int newEl, JsonDiff && diff)
        : JsonDiff(std::move(diff)), oldEl(oldEl), newEl(newEl)
    {
    }

    void reverse()
    {
        JsonDiff::reverse();
        std::swap(oldEl, newEl);
    }

    int oldEl;
    int newEl;
};



/*****************************************************************************/
/* JSON DIFF DESCRIPTION                                                     */
/*****************************************************************************/

DECLARE_STRUCTURE_DESCRIPTION(JsonDiff);
DECLARE_STRUCTURE_DESCRIPTION(JsonArrayElementDiff);

/** Create a JsonDiff between the two values. */
JsonDiff jsonDiff(const Json::Value & val1,
                  const Json::Value & val2,
                  bool strict = true);

/** Apply the patch to the given value. */
std::pair<std::unique_ptr<Json::Value>, JsonDiff>
jsonPatch(const Json::Value * val,
          const JsonDiff & diff);

inline std::pair<Json::Value, JsonDiff>
jsonPatch(const Json::Value & val,
          const JsonDiff & diff)
{
    auto res = jsonPatch(&val, diff);
    if (!res.first)
        res.first.reset(new Json::Value());
    return std::make_pair(std::move(*res.first), std::move(res.second));
}

} // namespace MLDB
