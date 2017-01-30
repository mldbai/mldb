// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* json_diff.cc
   Jeremy Barnes, 2 November 2013
   Copyright (c) 2013 mldb.ai inc.  All rights reserved.
*/

#include "json_diff.h"
#include "mldb/types/value_description.h"
#include "mldb/types/structure_description.h"
#include "mldb/types/map_description.h"
#include "mldb/types/vector_description.h"
#include "mldb/types/pointer_description.h"
#include "mldb/types/pair_description.h"
#include "mldb/base/parse_context.h"

#include <boost/regex.hpp>

using namespace std;

namespace MLDB {

JsonDiff::Deleted JsonDiff::deleted;


/*****************************************************************************/
/* JSON DIFF                                                                 */
/*****************************************************************************/

void
JsonDiff::
reverse()
{
    std::swap(oldValue, newValue);

    for (auto & f: fields)
        f.second.reverse();
    for (auto & e: elements)
        e.reverse();
}

/** Ignore the given values.  The regex will be called with strings like this:
        
    key1.key2.value:+new,-old

    Any which the regex matches against that string will be removed from the
    diff.
*/
void
JsonDiff::
ignore(const std::string & toIgnoreRegex, bool debug)
{
    boost::regex regex(toIgnoreRegex);

    std::function<bool (JsonDiff & diff, const std::string & path)> process
        = [&] (JsonDiff & diff, const std::string & path) -> bool
        {
            if (!diff.fields.empty()) {
                std::vector<string> fieldsToRemove;
                for (auto & f: diff.fields) {
                    bool remove = process(f.second, (path.empty() ? "": path + ".") + f.first);
                    if (remove)
                        fieldsToRemove.push_back(f.first);
                }

                for (auto & r: fieldsToRemove) {
                    diff.fields.erase(r);
                }

                return diff.fields.empty();
            }
            else if (!diff.elements.empty()) {
                // Reverse order so we can erase as we go
                for (int i = diff.elements.size() - 1;  i >= 0;  --i) {
                    auto & f = diff.elements[i];
                    bool remove = process(f, path + "[" + to_string(f.oldEl) + "]");
                    if (remove)
                        diff.elements.erase(diff.elements.begin() + i);
                }
                return diff.elements.empty();
            }
            else {
                string toMatch = path + ":";

                if (diff.oldValue) {
                    toMatch += "-[" + diff.oldValue->toStringNoNewLine() + "]";
                }
                if (diff.newValue) {
                    toMatch += "+[" + diff.newValue->toStringNoNewLine() + "]";
                }

                //cerr << "testing " << toMatch << endl;

                return boost::regex_match(toMatch, regex);
            }
        };

    process(*this, "");
}

JsonDiff
JsonDiff::
ignored(const std::string & toIgnoreRegex, bool debug) const
{
    JsonDiff result(*this);
    result.ignore(toIgnoreRegex, debug);
    return result;
}

/*****************************************************************************/
/* FREE FUNCTIONS                                                            */
/*****************************************************************************/

bool compareObjects(const Json::Value & val1,
                    const Json::Value & val2,
                    bool strict)
{
    if (strict)
        return val1 == val2;

    if (val1.type() == val2.type()) {
        switch (val1.type()) {
        case Json::objectValue: {
            vector<string> members1 = val1.getMemberNames();
            vector<string> members2 = val2.getMemberNames();

            if (members1 != members2)
                return false;
            
            for (auto && m: members1) {
                if (!compareObjects(val1[m], val2[m], strict))
                    return false;
            }

            return true;
        }

        case Json::arrayValue: {
            if (val1.size() != val2.size())
                return false;

            for (unsigned i = 0;  i < val1.size();  ++i) {
                if (!compareObjects(val1[i], val2[i], strict))
                    return false;
            }

            return true;
        }
        default:
            return val1 == val2;
        }
    }
    if (val1.isNumeric() && val2.isNumeric()
        && val1.asDouble() == val2.asDouble()) {
        return true;
    }
    return false;
}

JsonDiff
jsonDiffObjects(const Json::Value & obj1,
                const Json::Value & obj2,
                bool strict)
{
    JsonDiff result;

    auto members1 = obj1.getMemberNames(), members2 = obj2.getMemberNames();

    std::sort(members1.begin(), members1.end());
    std::sort(members2.begin(), members2.end());

    auto it1 = members1.begin(), end1 = members1.end();
    auto it2 = members2.begin(), end2 = members2.end();
    
    while (it1 != end1 || it2 != end2) {
        if (it1 != end1 && it2 != end2 && *it1 == *it2) {
            // Same name

            auto diff = jsonDiff(obj1[*it1], obj2[*it2], strict);

            if (diff)
                result.fields[*it1] = std::move(diff);
            
            ++it1;
            ++it2;
        }
        else if (it1 != end1 && (it2 == end2 || *it1 < *it2)) {
            // Deleted field
            result.fields[*it1] = JsonDiff(obj1[*it1], JsonDiff::deleted);
            ++it1;
        }
        else {
            // Inserted field
            result.fields[*it2] = JsonDiff(JsonDiff::deleted, obj2[*it2]);
            ++it2;
        }
    }

    return result;
}

/** Find the edit distance */

struct DistanceEntry {
    DistanceEntry(double dist = 0)
        : dist(dist), ins(0), del(0), sub(0)
    {
    }

    double dist; ///< Distance

    int ins:1;   ///< Insertion is optimal
    int del:1;   ///< Deletion is optimal
    int sub:1;   ///< Substitution is optimal
};

vector<vector<DistanceEntry> >
getDistances(const Json::Value & arr1, const Json::Value & arr2,
             bool debug, bool strict)
{
    int m = arr1.size(), n = arr2.size();

    if (debug) {
        cerr << "getDistances: arr1 = " << endl << arr1 << " arr2 = "
             << endl << arr2 << endl;
    }

    vector<vector<DistanceEntry> > result(m + 1,
                                          vector<DistanceEntry>(n + 1));
    
    // Initialize with insertion only
    for (unsigned i = 0;  i <= m;  ++i) {
        result[i][0] = i;
    }

    for (unsigned j = 0;  j <= n;  ++j) {
        result[0][j] = j;
    }

    double minDistLastColumn = 0;
    double beamWidth = 1000;

    // Now create the table
    for (unsigned j = 0;  j < n && m > 0;  ++j) {
        if (debug)
            cerr << "j = " << j << " of " << n << endl;

        double minDistThisColumn = INFINITY;

        for (unsigned i = 0;  i < m;  ++i) {
            if (debug)
                cerr << "i = " << i << " of " << m << endl;

            DistanceEntry dist;

            if (debug)
                cerr << "entry " << i << "," << j << endl;

            if (result[i][j].dist > minDistThisColumn + beamWidth) {
                dist.dist = INFINITY;
            }
            else if (compareObjects(arr1[i], arr2[j], strict)) {
                // Same element
                dist.dist = result[i][j].dist;
                if (debug)
                    cerr << "  ** SAME; dist = " << dist.dist << endl;
            }
            else {
                double deletionDist     = result[i][j + 1].dist + 1;
                double insertionDist    = result[i + 1][j].dist + 1;
                double substitutionDist = result[i][j].dist + 1;

                dist = std::min(std::min(deletionDist,
                                         insertionDist),
                                substitutionDist);

                dist.ins = (insertionDist == dist.dist);
                dist.del = (deletionDist  == dist.dist);
                dist.sub = (substitutionDist == dist.dist);

                if (debug) {
                    cerr << "  ** del " << deletionDist << " ins "
                         << insertionDist << " sub " << substitutionDist
                         << " dist " << dist.dist << endl;
                }
            }

            result[i + 1][j + 1] = dist;
            minDistThisColumn = std::min(minDistThisColumn, dist.dist);
        }

        if (!isfinite(minDistThisColumn)) {
            if (!debug)
                getDistances(arr1, arr2, true, strict);
        }

        ExcAssert(std::isfinite(minDistThisColumn));

        minDistLastColumn = minDistThisColumn;
    }

    if (debug) 
        cerr << "distance is " << result[m][n].dist << " of " << m << "," << n << endl;

    return result;
}

JsonDiff
jsonDiffArrays(const Json::Value & arr1,
               const Json::Value & arr2,
               bool strict)
{
    bool debug = false;

    if (debug) {
        cerr << "arr1 = " << arr1.toString();
        cerr << "arr2 = " << arr2.toString();
    }

    if (compareObjects(arr1, arr2, strict)) {
        return JsonDiff();
    }

#if 0
    if (arr1.size() == arr2.size()
        && arr1.toString() == arr2.toString()) {
        cerr << "Warning: different in comparison but same printed" << endl;
        cerr << arr1 << arr2 << endl;
        return JsonDiff();
    }
#endif

    JsonDiff result;

    // TODO: this is a hack; fix it
    return JsonDiff(arr1, arr2);

    auto d = getDistances(arr1, arr2, debug, strict);

    int m = arr1.size(), n = arr2.size();

    int i = m - 1, j = n - 1;

    double totalDist = d[m][n].dist;

    if (debug) {
        cerr << "totalDist = " << totalDist << endl;
    }

    while (i >= 0 || j >= 0) {
        ExcAssertLessEqual(d[i + 1][j + 1].dist, totalDist);

        if (debug) {
            if (/*arr1[i] != arr2[j] ||*/ true) {
                cerr << "i = " << i << " j = " << j;
                if (i >= 0)
                    cerr << " arr1[i] = " << arr1[i].toString();
                if (j >= 0)
                    cerr << " arr2[j] = " << arr2[j].toString();
                cerr << " dist1 "
                     << d[i + 1][j + 1].dist;
                if (i >= 0 && j >= 0)
                    cerr << " dist2 " << d[i][j].dist;
                cerr << (d[i + 1][j + 1].sub ? "sub " : "")
                     << (d[i + 1][j + 1].ins ? "ins " : "")
                     << (d[i + 1][j + 1].del ? "del " : "")
                     << endl;
            }
        }

        if (i >= 0 && j >= 0 && d[i + 1][j + 1].sub) {
            if (debug)
                cerr << "subst" << endl;

            ExcAssert(i >= 0);
            ExcAssert(j >= 0);
            // substitution
            result.elements.emplace_back(i, j,
                                         jsonDiff(arr1[i], arr2[j], strict));

            --i;
            --j;
        }
        else if (i == -1 || d[i + 1][j + 1].ins) {
            ExcAssert(j >= 0);
            if (debug)
                cerr << "ins" << endl;
            // insertion
            result.elements.emplace_back(i, j, 
                                         JsonDiff(JsonDiff::deleted, arr2[j]));
            --j;
        }
        else if (j == -1 || d[i + 1][j + 1].del) {
            ExcAssert(i >= 0);
            if (debug)
                cerr << "del" << endl;
            // deletion
            result.elements.emplace_back(i, j,
                                         JsonDiff(arr1[i], JsonDiff::deleted));
            --i;
        }
        else {
            if (debug)
                cerr << "skip" << endl;

            ExcAssert(i >= 0);
            ExcAssert(j >= 0);
            if (arr1[i] != arr2[j]) {
                cerr << "arr1 = " << arr1 << endl;
                cerr << "arr2 = " << arr2 << endl;
            }
            ExcAssertEqual(arr1[i], arr2[j]);
            // No difference; nothing to do
            --i;
            --j;
        }

    }

    // They were backwards; put them in the right order
    std::reverse(result.elements.begin(), result.elements.end());

    if (debug) {
        cerr << "i = " << i << " j = " << j << endl;
    }

    ExcAssertEqual(i, -1);
    ExcAssertEqual(j, -1);

    return result; //JsonDiff(arr1, arr2);
}

JsonDiff
jsonDiff(const Json::Value & val1,
         const Json::Value & val2,
         bool strict)
{
    if (compareObjects(val1, val2, strict))
        return JsonDiff();  // no difference
    
    if (val1.type() == val2.type()) {
        switch (val1.type()) {
        case Json::objectValue:
            return jsonDiffObjects(val1, val2, strict);
        case Json::arrayValue:
            return jsonDiffArrays(val1, val2, strict);
        default:
            return JsonDiff(val1, val2);
        }
    }

    // Different types
    return JsonDiff(val1, val2);
}

std::pair<std::unique_ptr<Json::Value>, JsonDiff>
jsonPatch(const Json::Value * val,
          const JsonDiff & diff)
{
    std::unique_ptr<Json::Value> patchedValue;
    JsonDiff conflicts;

    //cerr << "diff is = " << jsonEncodeStr(diff) << endl;

    // No differences; return what we were passed in
    if (!diff) {
        if (val)
            patchedValue.reset(new Json::Value(*val));
    }
    else if (diff.elements.size()) {
        // Array
#if 1
        if (!val || !val->isArray())
            conflicts = diff;
        else {
            cerr << endl;
            cerr << "diff is " << diff << endl;
            cerr << "array patch with " << diff.elements.size()
                 << " elements" << endl;

            std::vector<Json::Value> newVals(val->begin(), val->end());

            for (auto it = diff.elements.rbegin();  it != diff.elements.rend();
                 ++it) {

                cerr << "applying patch: oldEl = " << it->oldEl
                     << " newEl = " << it->newEl << endl;
                

                const JsonDiff & eldiff = *it;

                if (!eldiff.oldValue) {
                    ExcAssert(eldiff.newValue);

                    int pos = it->newEl;
                    if (pos == -1) {
                        // insert at end
                        newVals.insert(newVals.end(), *eldiff.newValue);
                    }
                    else if (pos < 0 || pos > newVals.size()) {
                        cerr << "*** insert conflict at " << pos << endl;
                        conflicts.elements.push_back(*it);
                    }
                    else {
                        newVals.insert(newVals.begin() + pos,
                                       *eldiff.newValue);
                    }
                }
                else if (!eldiff.newValue) {
                    // erased

                    int pos = it->oldEl;

                    ExcAssert(eldiff.oldValue);
                    if (pos < 0 || pos >= newVals.size()
                        || newVals[pos] != *eldiff.oldValue) {
                        cerr << "*** erase conflict at " << pos << endl;
                        conflicts.elements.push_back(*it);
                    }
                    else {
                        newVals.erase(newVals.begin() + pos);
                    }
                }
                else {
                    // modified

                    int pos = it->oldEl;

                    if (pos < 0 || pos >= newVals.size()) {
                        cerr << "*** modify conflict at " << pos << endl;
                        conflicts.elements.push_back(*it);
                    }
                    else {
                        std::unique_ptr<Json::Value> newVal;
                        JsonDiff elConflicts;

                        std::tie(newVal, elConflicts)
                            = jsonPatch(&newVals[pos], eldiff);
                        if (newVal)
                            newVals[pos] = std::move(*newVal);
                        if (elConflicts)  {
                            conflicts.elements.emplace_back(it->oldEl, it->newEl, std::move(elConflicts));
                            cerr << "*** patch conflict at " << pos << endl;
                            cerr << "newVals[pos] = " << newVals[pos] << endl;
                            cerr << "eldiff = " << eldiff << endl;
                        }
                    }
                }
            }

            patchedValue.reset(new Json::Value(newVals.begin(), newVals.end()));
            std::reverse(conflicts.elements.begin(), conflicts.elements.end());
#else
        if (!val || !val->isArray()) {
            cerr << "dest val is nil or not an array\n";
            conflicts = diff;
        }
        else {
            patchedValue.reset(new Json::Value(*val));

            for (auto & f: diff.elements) {
                if (f.second.newValue) {
                    if (f.second.oldValue) {
                        // cerr << "value replacement\n";
                        const Json::Value * current = nullptr;
                        if (val->isValidIndex(f.first)) {
                            current = &(*val)[f.first];
                        }
                        else {
                            throw MLDB::Exception("element does not exist");
                        }

                        JsonDiff fieldConflicts;
                        std::unique_ptr<Json::Value> newVal;

                        std::tie(newVal, fieldConflicts)
                            = jsonPatch(current, f.second);
                        if (fieldConflicts)
                            conflicts.elements[f.first] = fieldConflicts;
                        (*patchedValue)[f.first].swap(*newVal);
                    }
                    else {
                        // cerr << "value insertion/addition\n";
                        size_t oldSize = patchedValue->size();
                        size_t newSize = oldSize + 1;
                        patchedValue->resize(newSize);
                        if (patchedValue->isValidIndex(f.first)) {
                            /* first, shift the remaining elements */
                            for (size_t i = newSize - 1; i > f.first; i--) {
                                (*patchedValue)[i] = move((*patchedValue)[i-1]);
                            }
                        }
                        else if (f.first > oldSize) {
                            throw MLDB::Exception("index " + to_string(f.first)
                                                + " is invalid");
                        }
                        (*patchedValue)[f.first] = *f.second.newValue;
                    }
                }
                else {
                    // cerr << "f.first: " << f.first
                    //      << "; f.second: " << f.second
                    //      << "; f.second.oldValue: " << *f.second.oldValue
                    //      << endl;
                    if (val->isValidIndex(f.first)
                        && (*val)[f.first] == *f.second.oldValue) {
                        size_t newSize = patchedValue->size() - 1;
                        for (int i = f.first; i < newSize; i++) {
                            (*patchedValue)[i] = move((*patchedValue)[i+1]);
                        }
                        patchedValue->resize(newSize);
                    }
                    else {
                        conflicts.elements[f.first] = f.second;
                    }
                }
            }
#endif
        }
    }
    else if (diff.fields.size()) {
        // Object
        if (!val || !val->isObject())
            conflicts = diff;
        else {
            patchedValue.reset(new Json::Value(*val));

            //cerr << "starting object: " << jsonEncodeStr(*patchedValue)
            //     << endl;

            for (auto & f: diff.fields) {
                const Json::Value * current = 0;
                if (val->isMember(f.first))
                    current = &(*val)[f.first];

                JsonDiff fieldConflicts;
                std::unique_ptr<Json::Value> newVal;

                //cerr << "field " << f.first << " " << jsonEncodeStr(f.second)
                //     << " newVal "
                //     << (newVal ? *newVal: "<null>") << endl;

                std::tie(newVal, fieldConflicts)
                    = jsonPatch(current, f.second);

                if (fieldConflicts)
                    conflicts.fields[f.first] = fieldConflicts;
                
                if (newVal && current)
                    (*patchedValue)[f.first].swap(*newVal);
                else if (newVal)
                    (*patchedValue)[f.first] = std::move(*newVal);
                else {
                    patchedValue->removeMember(f.first);
                }
            }
            //cerr << "finished object: " << jsonEncodeStr(*patchedValue)
            //     << endl;
        }

    }
    else if (diff.oldValue) {
        // There was an old value; if it matches we replace with the new
        // value
        if (val && *diff.oldValue == *val) {
            // It matches.  Is there a new value, or should we erase it
            // completely?
            if (diff.newValue) {
                // has a new value
                patchedValue.reset(new Json::Value(*diff.newValue));
            }
        }
        // TODO: Already applied?
        else {
            conflicts = diff;
        }
    }
    else if (diff.newValue) {
        // There was no old value but a new value.  Check that there is
        // no current value.
        if (val) {
            // There was an old value.  This is a conflict.
            // TODO: Already applied
            conflicts = diff;
        }
        else {
            // There was no old value either.  Becomes a new value
            patchedValue.reset(new Json::Value(*diff.newValue));
        }
    }
    else {
        throw MLDB::Exception("logic error");
    }

    return make_pair(std::move(patchedValue), std::move(conflicts));
}


/*****************************************************************************/
/* JSON DIFF DESCRIPTION                                                     */
/*****************************************************************************/

DEFINE_STRUCTURE_DESCRIPTION(JsonArrayElementDiff);
DEFINE_STRUCTURE_DESCRIPTION(JsonDiff);

JsonDiffDescription::
JsonDiffDescription()
{
    addField("f", &JsonDiff::fields,
             "differences per-field");
    addField("el", &JsonDiff::elements,
             "differences per array element");
    addField("-", &JsonDiff::oldValue,
             "old (previous) value of this element");
    addField("+", &JsonDiff::newValue,
             "new (updated) value of this element");
}

std::ostream & operator << (std::ostream & stream, const JsonDiff & diff)
{
    return stream << jsonEncode(diff);
}

JsonArrayElementDiffDescription::
JsonArrayElementDiffDescription()
{
    addParent<JsonDiff>();
    addField("-idx", &JsonArrayElementDiff::oldEl,
             "Index of element in old array", -2);
    addField("+idx", &JsonArrayElementDiff::newEl,
             "Index of element in new array", -2);
}


} // namespace MLDB
