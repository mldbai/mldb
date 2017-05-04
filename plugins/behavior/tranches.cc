// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* tranches.cc
   Jeremy Barnes, 1 August 2013
   Copyright (c) 2013 mldb.ai inc.  All rights reserved.

*/

#include "tranches.h"
#include "mldb/jml/utils/vector_utils.h"
#include "mldb/base/parse_context.h"
#include "mldb/types/value_description.h"

using namespace std;


namespace MLDB {


/*****************************************************************************/
/* TRANCHE SPEC                                                              */
/*****************************************************************************/

TrancheSpec::
TrancheSpec()
    : set(1, 0),
      modulusShift(0)
{
}

TrancheSpec::
TrancheSpec(int totalTranches)
{
    modulusShift = ML::highest_bit(totalTranches, -1);
    ExcAssertEqual(this->totalTranches(), totalTranches);
    for (unsigned i = 0;  i < totalTranches;  ++i)
        set.push_back(i);
}

TrancheSpec::
TrancheSpec(const std::string & str)
{
    *this = parse(str);
}

TrancheSpec::
TrancheSpec(const char * str)
{
    *this = parse(str);
}

TrancheSpec::
TrancheSpec(int num, int totalTranches)
{
    //cerr << "num = " << num << " totalTranches = " << totalTranches
    //     << endl;

    ExcAssertGreaterEqual(num, 0);
    ExcAssertLess(num, totalTranches);
    int m = ML::highest_bit(totalTranches, -1) + 1;

    if (m == 0)
        throw MLDB::Exception("totalTranches can't be 0");
    if (m > 65536)
        throw MLDB::Exception("totalTranches can't be > 65536");

    modulusShift = m - 1;

    this->set.push_back(num);
}

TrancheSpec::
TrancheSpec(const std::vector<int> & set, int totalTranches)
{
    if (set.empty()) {
        modulusShift = 0;
        return;
    }

    //cerr << "tranche spec " << set << " " << totalTranches << endl;

    int m = ML::highest_bit(totalTranches, -1) + 1;

    if (m == 0)
        throw MLDB::Exception("totalTranches can't be 0");
    if (m > 65536)
        throw MLDB::Exception("totalTranches can't be > 65536");

    modulusShift = m - 1;

    for (auto t: set) {
        if (t < 0 || t >= totalTranches)
            throw MLDB::Exception("invalid tranche number in TrancheSpec");
    }
    
    this->set = set;

    std::sort(this->set.begin(), this->set.end());
}

void
TrancheSpec::
infer(const BehaviorDomain & behs,
      int maxModulusShift)
{
    int tryTranches = 1 << maxModulusShift;

    //cerr << "inferring " << tryTranches << endl;

    vector<int> knownTranches(tryTranches);  // TODO: more than 256 tranches
    int tranchesPresent = 0;

    auto onSubject = [&] (SH subject, const SubjectIterInfo &)
        {
            int tranche = subject.hash() % tryTranches;
            if (knownTranches[tranche])
                return true;
            knownTranches[tranche] = true;
            ++tranchesPresent;
            return tranchesPresent < tryTranches;
        };

    behs.forEachSubject(onSubject);

    //cerr << "knownTranches = " << knownTranches << endl;

    set.clear();

    for (unsigned i = 0;  i < knownTranches.size();  ++i)
        if (knownTranches[i])
            set.push_back(i);

    modulusShift = maxModulusShift;

    collapse();
}

bool
TrancheSpec::
tryCollapseOneLevel()
{
    //cerr << "tryCollapseOneLevel " << *this << " modulusShift = " << modulusShift
    //     << endl;

    if (modulusShift == 0)
        return false;

    int newModulusShift = modulusShift - 1;
    int oldNumTranches = 1 << modulusShift;
    int newNumTranches = 1 << newModulusShift;

    vector<int> oldBitmap(oldNumTranches, 0);
    for (auto tr: set)
        oldBitmap.at(tr) = 1;

    //cerr << "oldBitmap = " << oldBitmap << endl;

    vector<int> newSet;

    for (unsigned i = 0;  i < newNumTranches;  ++i) {
        int val0 = oldBitmap[i];
        int val1 = oldBitmap[i + newNumTranches];

        if (val0 != val1)
            return false;
            
        if (val0)
            newSet.push_back(i);
    }

    modulusShift = newModulusShift;
    set.swap(newSet);

    return true;
}

void
TrancheSpec::
expandOneLevel()
{
    std::vector<int> newSet(set);
    newSet.resize(set.size() * 2);
    for (unsigned i = 0;  i < set.size();  ++i)
        newSet[i + set.size()] = set[i] + totalTranches();
    set.swap(newSet);
    modulusShift += 1;
}

void
TrancheSpec::
collapse(int newModulusShift)
{
    while (newModulusShift == -1 || modulusShift > newModulusShift)
        if (!tryCollapseOneLevel())
            return;
}

std::string
TrancheSpec::
toString(char separator) const
{
    if (set.empty())
        return "0";


    string total = std::to_string(1 << modulusShift);
    string fmt = MLDB::format("%%0%zdd", total.length());

    string result;

    // If all tranches, use a * for shorthand
    if (set.size() == (1 << modulusShift) && modulusShift > 0) {
        result = "*";
        result += separator;
    }
    else {
        // detect contiguous ranges and make them like 1-4
        for (auto it = set.begin(), end = set.end();  it != end;  /* no inc */) {
            int start = *it++;
            int finish = start;
            while (it != end && *it == finish + 1)
                finish = *it++;
            if (start == finish)
                result += MLDB::format(fmt.c_str(), start);
            else result += MLDB::format(fmt.c_str(), start) + '-' + MLDB::format(fmt.c_str(), finish);
            result += ',';
        }

        // Replace the last _ with the separator
        result[result.size() - 1] = separator;
    }

    result += total;

    return result;
}

TrancheSpec
TrancheSpec::
parse(const std::string & val, char separator)
{
    //cerr << "parsing " << val << endl;

    std::vector<int> newSet;

    if (val == "0")
        return TrancheSpec(newSet, 0);

    try {
        ParseContext context(val, val.c_str(), val.c_str() + val.size());

        // *_32 means all of 32 tranches
        if (context.match_literal('*')) {
            context.expect_literal(separator);
            int totalTranches = context.expect_int();
            for (unsigned i = 0;  i < totalTranches;  ++i)
                newSet.push_back(i);
            return TrancheSpec(newSet, totalTranches);
        }

        while (*context != separator) {
            int val = context.expect_int();
            if (context.match_literal('-')) {
                int endRange = context.expect_int();
                if (endRange < val || val + 1000000 < endRange)
                    context.exception(MLDB::format("invalid tranche range: %d-%d", val, endRange));
                for (int i = val;  i <= endRange;  ++i)
                    newSet.push_back(i);
            }
            else {
                newSet.push_back(val);
            }
            if (!context.match_literal(','))
                break;
        }

        context.expect_literal(separator);

        int totalTranches = context.expect_int();
    
        return TrancheSpec(newSet, totalTranches);
    } catch (const std::exception & exc) {
        throw MLDB::Exception("parsing tranche spec '" + val + "': "
                            + exc.what());
    }
}

bool
TrancheSpec::
intersects(const TrancheSpec & other) const
{
#if 0
    if (matchesAll() || other.matchesAll())
        return true;

    if (modulusShift != other.modulusShift) { 
        cerr << "me: " << *this << endl;
        cerr << "other: " << other << endl;
        throw MLDB::Exception("modulus shift matching not done: %d vs %d",
                            modulusShift, other.modulusShift);
    }
#endif

    if (modulusShift > other.modulusShift)
        return other.intersects(*this);

    if (modulusShift == other.modulusShift) {
        // Simplest: comparison
        if (set.size() == 1 && other.set.size() == 1)
            return set[0] == other.set[0];

        // Simple: co-iteration
        int i0 = 0, i1 = 0, e0 = set.size(), e1 = other.set.size();

        while (i0 < e0 && i1 < e1) {
            int v0 = set[i0];
            int v1 = other.set[i1];

            if (v0 == v1)
                return true;
            if (v0 < v1)
                ++i0;
            else ++i1;
        }

        return false;
    }

    // Our modulus shift is less than the other one.  So all we need to
    // do is map the other one's values to our modulus, and then
    // check to see if any of them hit our values

    // TODO: optimize me

    std::set<int> otherMapped;
    int mask = (1 << modulusShift) - 1;

    for (auto s: other.set) {
        int m = s & mask;
        otherMapped.insert(m);
    }

    std::vector<int> inter;
    std::set_intersection(set.begin(), set.end(),
                          otherMapped.begin(), otherMapped.end(),
                          std::back_inserter(inter));
    //cerr << "me: " << *this << endl;
    //cerr << "other: " << other << endl;
    //cerr << "inter: " << inter << endl;
    return !inter.empty();
}

TrancheSpec
TrancheSpec::
complement() const
{
    if (totalTranches() == 0)
        return TrancheSpec(0,1);

    vector<int> complement_set;
    for (int i=0; i < totalTranches(); ++i)
        if (!count(i))
            complement_set.push_back(i);

    return TrancheSpec(complement_set, totalTranches());
}

TrancheSpec
TrancheSpec::
atModulus(int newModulusShift) const
{
    if (newModulusShift == modulusShift)
        return *this;
    else if (newModulusShift < modulusShift) {
        TrancheSpec result = *this;
        while (newModulusShift < result.modulusShift)
            result.tryCollapseOneLevel();
        return result;
    }
    
    TrancheSpec result = *this;
    while (newModulusShift > result.modulusShift) {
        result.expandOneLevel();
    }

    return result;
}

TrancheSpec
TrancheSpec::
intersect(const TrancheSpec & other) const
{
    int maxModulus = std::max(modulusShift, other.modulusShift);
    TrancheSpec t1 = this->atModulus(maxModulus);
    TrancheSpec t2 = other.atModulus(maxModulus);

    vector<int> entries;
    std::set_intersection(t1.set.begin(), t1.set.end(),
                          t2.set.begin(), t2.set.end(),
                          std::back_inserter(entries));

    TrancheSpec result;
    result.set = std::move(entries);
    result.modulusShift = maxModulus;
    return result;
}

bool
TrancheSpec::
operator==(const TrancheSpec & other) const
{
    int maxModulus = std::max(this->modulusShift, other.modulusShift);
    TrancheSpec t1 = this->atModulus(maxModulus);
    TrancheSpec t2 = other.atModulus(maxModulus);
    vector<int> s1(t1.set.begin(), t1.set.end());
    vector<int> s2(t2.set.begin(), t2.set.end());
    return (s1 == s2);
}

std::ostream &
operator << (std::ostream & stream, const TrancheSpec & spec)
{
    return stream << spec.toString();
}

struct TrancheSpecDescription: public ValueDescriptionT<TrancheSpec> {

    virtual void parseJsonTyped(TrancheSpec * val,
                                JsonParsingContext & context) const
    {
        *val = TrancheSpec::parse(context.expectStringAscii());
    }

    virtual void printJsonTyped(const TrancheSpec * val,
                                JsonPrintingContext & context) const
    {
        context.writeString(val->toString());
    }
};

ValueDescriptionT<TrancheSpec> * getDefaultDescription(TrancheSpec *)
{
    return new TrancheSpecDescription();
}


} // namespace MLDB
