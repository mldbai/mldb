// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/** boolean_expression.cc
    Jeremy Barnes, 21 August 2012
    Copyright (c) 2012 mldb.ai inc.  All rights reserved.

*/

#include "boolean_expression.h"
#include "mldb/arch/exception.h"
#include "mldb/arch/format.h"
#include <sstream>
#include "boost/lexical_cast.hpp"
#include "boolean_expression_parser.h"
#include <boost/regex.hpp>
#include <mutex>
#include "mldb/base/parallel.h"

using namespace std;

namespace MLDB {


/******************************************************************************/
/* UTILS                                                                      */
/******************************************************************************/

namespace {

std::string printId(const Id& id)
{
    if (id.type == Id::BIGDEC || id.type == Id::INT64DEC)
        return id.toString();
    return "\"" + id.toString() + "\"";
}

} // namespace anonymous


/*****************************************************************************/
/* BEHAVIOR WRAPPER                                                         */
/*****************************************************************************/

BehaviorWrapper::
BehaviorWrapper(const BehaviorDomain & behs)
    : behs(&behs)
{
    using namespace std::placeholders;

    allSubjectHashes = std::bind(mem_fn(&BehaviorDomain::allSubjectHashes),
                                 this->behs, _1, true /* sorted */);
    
    getSubjectHashesAndTimestamps
        = std::bind(mem_fn(&BehaviorDomain::getSubjectHashesAndTimestamps),
                    this->behs, _1, _2, true /* sorted */);
                           
    getSubjectHashesAndAllTimestamps
        = std::bind(mem_fn(&BehaviorDomain::getSubjectHashesAndAllTimestamps),
                    this->behs, _1, _2, true /* sorted */);

    getBehaviorsContainingString = [=] (const std::string & mustContain)
        {
            std::vector<Id> segmentsMatching;

            auto onBeh = [&] (BH beh, const BehaviorIterInfo & info,
                              const BehaviorStats & stats)
            {
                if (stats.id.toString().find(mustContain)!=string::npos)
                    segmentsMatching.push_back(stats.id);
                return true;
            };
            
            this->behs->forEachBehaviorParallel(onBeh, BehaviorDomain::ALL_BEHAVIORS,
                                          BehaviorDomain::SS_ID,
                                          ORDERED);

            return segmentsMatching;
        };

    getBehaviorsMatchingRegex = [=] (const std::string & regex)
        {
            std::vector<Id> segmentsMatching;

            auto onBeh = [&] (BH beh, const BehaviorIterInfo & info,
                              const BehaviorStats & stats)
            {
                segmentsMatching.push_back(stats.id);
                return true;
            };
    
            BehaviorFilter filter;
            filter.filterByRegex(regex);

            this->behs->forEachBehaviorParallel(onBeh, filter, BehaviorDomain::SS_ID,
                                          ORDERED);

            return segmentsMatching;
        };
}

BehaviorWrapper::
BehaviorWrapper()
{
}


/*****************************************************************************/
/* BOOLEAN EXPRESSION                                                        */
/*****************************************************************************/

BoolExprPtr
BooleanExpression::
parse(const std::string & expression)
{
    static const BooleanExpressionParser parser;
    return parser.parse(expression);
}

BoolExprPtr
BooleanExpression::
bind(const BehaviorWrapper & behs,
     const boost::any & md) const
{
    return makeCopy();
}

std::vector<std::pair<SH, Date> >
BooleanExpression::
generateUnique(const BehaviorWrapper & behs,
               SH maxSubject)
{
    std::vector<std::pair<SH, Date>> gen = generate(behs, maxSubject);
    std::vector<std::pair<SH, Date>> result;
    // Make sure it's sorted
    std::sort(gen.begin(), gen.end());
    for (auto sh_date : gen)
        if (result.size() && sh_date.first == result.back().first)
            result.back().second = std::min(result.back().second,
                                            sh_date.second);
        else
            result.push_back(sh_date);
    return result;
}

struct CompareSubjects {
    bool operator () (const std::pair<SH, Date> & p1,
                      const std::pair<SH, Date> & p2) const
    {
        return p1.first < p2.first;
    }
};

static std::vector<std::pair<SH, Date> >
doDifference(const std::vector<std::pair<SH, Date> > & input,
             const std::vector<std::pair<SH, Date> > & toExclude)
{
    std::vector<std::pair<SH, Date> > result;

    std::set_difference(input.begin(), input.end(),
                        toExclude.begin(), toExclude.end(),
                        back_inserter(result),
                        CompareSubjects());

    return result;
}

template<typename f, typename f2>
static std::vector<std::pair<SH, Date> >
doIntersection(const std::vector<std::pair<SH, Date> > & input1,
               const std::vector<std::pair<SH, Date> > & input2,
               f accept, f2 pickDate)
{
    std::vector<std::pair<SH, Date> > result;

    auto it1 = input1.begin(), end1 = input1.end();
    auto it2 = input2.begin(), end2 = input2.end();

    while (it1 != end1 && it2 != end2) {
        auto s1 = it1->first;
        auto s2 = it2->first;

        if (s1 == s2) {
            if (accept(it1->second, it2->second)) {
                Date d = pickDate(it1->second, it2->second);
                result.push_back(make_pair(s1, d));
            }
            ++it1;
            ++it2;
        }
        else if (s1 < s2)
            ++it1;
        else ++it2;
    }

    return result;
}

/// Take the union, only keeping the first timestamp
/// SELECT uid, min(timestamps)
/// PRE: input1 and input2 are sorted, uniquified and have 1 entry per uid
static std::vector<std::pair<SH, Date> >
doUnion(const std::vector<std::pair<SH, Date> > & input1,
        const std::vector<std::pair<SH, Date> > & input2)
{
    std::vector<std::pair<SH, Date> > result;
    result.reserve(input1.size() + input2.size());

    auto it1 = input1.begin(), end1 = input1.end();
    auto it2 = input2.begin(), end2 = input2.end();

    while (it1 != end1 && it2 != end2) {
        auto s1 = it1->first;
        auto s2 = it2->first;

        if (s1 < s2){ //most frequent case
            result.push_back(*it1);
            ++it1;
        }
        else if (s1 == s2) {
            Date d = std::min(it1->second, it2->second);
            result.push_back(make_pair(s1, d));
            ++it1;
            ++it2;
        }
        else {
            result.push_back(*it2);
            ++it2;
        }
    }

    result.insert(result.end(), it1, end1);
    result.insert(result.end(), it2, end2);

    return result;
}

/// Take the union, keeping all distinct timestamps
/// SELECT DISTINCT uid, timestamp
/// PRE: input1 and input2 are sorted and uniquified by (uid,ts)
static std::vector<std::pair<SH, Date> >
doUnionAll(const std::vector<std::pair<SH, Date> > & input1,
           const std::vector<std::pair<SH, Date> > & input2)
{
    std::vector<std::pair<SH, Date> > result;
    result.reserve(input1.size() + input2.size());

    std::set_union(input1.begin(), input1.end(),
                   input2.begin(), input2.end(),
                   back_inserter(result));
    
    return result;
}

std::vector<std::pair<SH, Date> >
BooleanExpression::
filter(const BehaviorWrapper & behs,
       const std::vector<std::pair<SH, Date> > & input) const
{
    if (input.empty())
        return std::vector<std::pair<SH, Date> >();
    SH maxSubject = input.back().first;

    auto accept = [] (const Date& d1, const Date& d2) -> bool
        {
            return true;
        };

    auto pickDate = [] (const Date& d1, const Date& d2)
        {
            return std::max(d1, d2);
        };
    
    return doIntersection(input, generate(behs, maxSubject), accept, pickDate);
}

std::vector<std::pair<SH, Date> >
BooleanExpression::
filter_after(const BehaviorWrapper & behs,
             const std::vector<std::pair<SH, Date> > & input) const
{
    return filter_after_within(behs, input, 
            std::numeric_limits<unsigned>::max());
}

std::vector<std::pair<SH, Date> >
BooleanExpression::
filter_after_within(const BehaviorWrapper & behs,
             const std::vector<std::pair<SH, Date> > & input,
             unsigned within) const
{
    if (input.empty())
        return std::vector<std::pair<SH, Date> >();
    SH maxSubject = input.back().first;

    auto accept = [&] (const Date& d1, const Date& d2) -> bool
        {
            return (d2>d1) && (d2-d1<within);
        };

    auto pickDate = [] (const Date& d1, const Date& d2)
        {
            return std::max(d1, d2);
        };

    return doIntersection(input, generate(behs, maxSubject), accept, pickDate);
}

/*****************************************************************************/
/* SEG EXPRESSION                                                            */
/*****************************************************************************/

SegExpression::
SegExpression(Id seg)
    : seg(seg)
{
}

std::string
SegExpression::
print() const
{
    return printId(seg);
}

std::string
SegExpression::
printSql() const
{
    return "\"" + seg.toString() + "\"";
}

std::vector<std::pair<SH, Date> >
SegExpression::
generate(const BehaviorWrapper & behs,
         SH maxSubject) const
{
    return behs.getSubjectHashesAndTimestamps(seg, maxSubject);
}

std::vector<std::pair<SH, Date> >
SegExpression::
generateAllTimestamps(const BehaviorWrapper & behs,
                      SH maxSubject) const
{
    return behs.getSubjectHashesAndAllTimestamps(seg, maxSubject);
}


/*****************************************************************************/
/* CONTAINS EXPRESSION                                                       */
/*****************************************************************************/

SegNameContainsExpression::
SegNameContainsExpression(const string & mC)
    : mustContain(mC)
{
}

std::string
SegNameContainsExpression::
print() const
{
    return "SEG_NAME_CONTAINS(" + mustContain + ")";
}

std::string
SegNameContainsExpression::
printSql() const
{
    throw MLDB::Exception("Not supported");
}

std::set<Id>
SegNameContainsExpression::
getSegments() const
{
    throw MLDB::Exception("Contains expression needs to be bound to a behaviors "
                        "to have a list of segments");
}

BoolExprPtr
SegNameContainsExpression::
bind(const BehaviorWrapper & behs,
     const boost::any & md) const
{
    // Find all segments matching
    std::vector<BoolExprPtr> segmentsMatching;

    for (const Id & id: behs.getBehaviorsContainingString(mustContain))
        segmentsMatching.push_back(std::make_shared<SegExpression>(id));

    return std::make_shared<OrExpression>(segmentsMatching);
}

std::vector<std::pair<SH, Date> >
SegNameContainsExpression::
generate(const BehaviorWrapper & behs,
         SH maxSubject) const
{
    // See PLAT-287
    // Fix is to call bind() on the expression and generate from its result
    cerr << "warning: SegNameContainsExpression should be bound, "
         << "not run directly" << endl;
    return bind(behs)->generate(behs, maxSubject);
}

std::vector<std::pair<SH, Date> >
SegNameContainsExpression::
generateAllTimestamps(const BehaviorWrapper & behs,
                      SH maxSubject) const
{
    throw MLDB::Exception("SegNameExpression can't be executed without binding");
}


/*****************************************************************************/
/* REGEX EXPRESSION                                                          */
/*****************************************************************************/

RegexExpression::
RegexExpression(const string & regex)
    : regex(regex)
{
}

std::string
RegexExpression::
print() const
{
    return "REGEX(" + regex + ")";
}

std::string
RegexExpression::
printSql() const
{
    throw MLDB::Exception("Not supported");
}

std::set<Id>
RegexExpression::
getSegments() const
{
    throw MLDB::Exception("Contains expression needs to be bound to a behaviors "
                        "to have a list of segments");
}

BoolExprPtr
RegexExpression::
bind(const BehaviorWrapper & behs,
     const boost::any & md) const
{
    // Find all segments matching
    std::vector<BoolExprPtr> segmentsMatching;

    for (const Id & id: behs.getBehaviorsMatchingRegex(regex))
        segmentsMatching.push_back(std::make_shared<SegExpression>(id));
    
    return std::make_shared<OrExpression>(segmentsMatching);
}

std::vector<std::pair<SH, Date> >
RegexExpression::
generate(const BehaviorWrapper & behs,
         SH maxSubject) const
{
    // See PLAT-287
    // Fix is to call bind() on the expression and generate from its result
    cerr << "warning: RegexExpression should be bound, not run directly" << endl;
    return bind(behs)->generate(behs, maxSubject);
}

std::vector<std::pair<SH, Date> >
RegexExpression::
generateAllTimestamps(const BehaviorWrapper & behs,
                      SH maxSubject) const
{
    throw MLDB::Exception("RegexExpression can't be executed without binding");
}


/******************************************************************************/
/* TIMES FUNCTION EXPRESSION                                                  */
/******************************************************************************/

TimesFunctionExpression::
TimesFunctionExpression(BoolExprPtr base, unsigned n) :
    base(std::move(base)), n(n)
{
    ExcAssert(this->base);
    ExcAssert(this->base->canGenerate());
    ExcAssertGreater(n, 0);
}

std::string
TimesFunctionExpression::
print() const
{
    return "TIMES(" + base->print() + "," + to_string(n) + ")";
}

std::string
TimesFunctionExpression::
printSql() const
{
    throw MLDB::Exception("Not supported");
}

std::vector<std::pair<SH, Date> >
TimesFunctionExpression::
generate(const BehaviorWrapper & behs, SH maxSubject) const
{
    /* Here, we generate by getting all timestamps from the
       inner expression, and looking for subjects that have
       n or more timestamps.

       We return the timestamp of the EARLIEST conversion, not
       the nth as you would expect.  This is to avoid biasing
       the model: normally you would use TIMES in order to
       say you want people who have done something more than
       once, but we don't want our models to use what happened
       between the different times, we want the model to learn
       what happened before the first one.
    */

    auto vals = (*base).generateAllTimestamps(behs, maxSubject);
    
    std::vector<std::pair<SH, Date> > result;

    SH lastSubj = SH();
    Date firstSeen;
    int numTimes = 0;

    for (const pair<SH, Date> & val: vals) {
        SH subj = val.first;
        Date ts = val.second;
        
        if (lastSubj == subj) {
            ++numTimes;
        }
        else {
            numTimes = 1;
            lastSubj = subj;
            firstSeen = ts;
        }

        if (numTimes == n) {
            result.push_back(make_pair(subj, firstSeen));
        }
    }

    return result;
}

std::vector<std::pair<SH, Date> >
TimesFunctionExpression::
generateAllTimestamps(const BehaviorWrapper & behs, SH maxSubject) const
{
    return generate(behs, maxSubject);
}

BoolExprPtr
TimesFunctionExpression::
bind(const BehaviorWrapper & behs,
     const boost::any & md) const
{
    auto res = std::static_pointer_cast<TimesFunctionExpression>(makeCopy());
    res->base = base->bind(behs, md);
    return res;
}


/*****************************************************************************/
/* NOT EXPRESSION                                                            */
/*****************************************************************************/

NotExpression::
NotExpression(BoolExprPtr base)
    : base(base)
{
}

std::string
NotExpression::
print() const
{
    return "NOT " + base->print();
}

std::string
NotExpression::
printSql() const
{
    return base->printSql() + " IS NULL";
}

std::vector<std::pair<SH, Date> >
NotExpression::
filter(const BehaviorWrapper & behs,
       const std::vector<std::pair<SH, Date> > & input) const
{
    if (input.empty())
        return std::vector<std::pair<SH, Date> >();

    SH maxSubject = input.back().first;

    return doDifference(input, base->generate(behs, maxSubject));
}

std::vector<std::pair<SH, Date> >
NotExpression::
generate(const BehaviorWrapper & behs, SH maxSubject) const
{
    vector<pair<SH,Date> > all;
    for (auto sh : behs.allSubjectHashes(maxSubject)) {
        if (sh > maxSubject)
            break;
        all.push_back(make_pair(sh, Date::negativeInfinity()));
    }
    return doDifference(all, base->generate(behs, maxSubject));
}

std::vector<std::pair<SH, Date> >
NotExpression::
generateAllTimestamps(const BehaviorWrapper & behs, SH maxSubject) const
{
    // We ignore the timestamps, so we can just use generate here
    return generate(behs, maxSubject);
}

BoolExprPtr 
NotExpression::
bind(const BehaviorWrapper & behs,
     const boost::any & md) const
{
    auto ne = std::dynamic_pointer_cast<NotExpression>(makeCopy());
    ne->base = base->bind(behs, md);
    return ne;
}

/*****************************************************************************/
/* AND EXPRESSION                                                            */
/*****************************************************************************/

AndExpression::
AndExpression(const std::vector<BoolExprPtr> & exprs)
    : CompoundExpression("AND", exprs)
{
    bool ok = false;
    for (auto e: exprs)
        ok = ok || e->canGenerate();
    if (!ok)
        throw MLDB::Exception("Need at least one expression that can generate");
}

std::vector<std::pair<SH, Date> >
AndExpression::
generate(const BehaviorWrapper & behs, SH maxSubject) const
{
    vector<std::pair<SH, Date> > result;

    int done = -1;
    for (unsigned i = 0;  i < exprs.size();  ++i) {
        if (exprs[i]->canGenerate()) {
            result = exprs[i]->generate(behs, maxSubject);
            done = i;
            break;
        }
    }

    for (unsigned i = 0;  i < exprs.size();  ++i) {
        if (i == done) continue;
        result = exprs[i]->filter(behs, result);
    }
    
    return result;
}

std::vector<std::pair<SH, Date> >
AndExpression::
filter(const BehaviorWrapper & behs,
       const std::vector<std::pair<SH, Date> > & input) const
{
    vector<std::pair<SH, Date> > result;

    for (unsigned i = 0;  i < exprs.size();  ++i) {
        result = exprs[i]->filter(behs, result);
    }

    return result;
}

std::vector<std::pair<SH, Date> >
AndExpression::
generateAllTimestamps(const BehaviorWrapper & behs, SH maxSubject) const
{
    throw MLDB::Exception("AndExpression::generateAllTimestamps(): "
                        "not done");
}


/*****************************************************************************/
/* OR EXPRESSION                                                             */
/*****************************************************************************/

OrExpression::
OrExpression(const std::vector<BoolExprPtr> & exprs)
    : CompoundExpression("OR", exprs)
{
    for (auto e: exprs)
        if (!e->canGenerate())
            throw MLDB::Exception("cannot put [" + e->print() + "] inside an OR");
}

std::vector<std::pair<SH, Date> >
OrExpression::
generate(const BehaviorWrapper & behs, SH maxSubject) const
{
    vector<pair<SH, Date> > result;
    mutex m;

    auto mainWork = [&](size_t it){
        vector<pair<SH, Date> > tmpResult = exprs[it].get()->generate(behs, maxSubject);
        if (tmpResult.size() > 0){
            unique_lock<mutex> lock(m);
            result = doUnion(result, tmpResult);
        }
    };
    parallelMap(0, exprs.size(), mainWork);

    return result;
}

std::vector<std::pair<SH, Date> >
OrExpression::
generateAllTimestamps(const BehaviorWrapper & behs, SH maxSubject) const
{
    vector<pair<SH, Date> > result;
    mutex m;

    auto mainWork = [&](size_t it){
        vector<pair<SH, Date> > tmpResult = exprs[it].get()->generateAllTimestamps(behs, maxSubject);
        if (tmpResult.size() > 0){
            unique_lock<mutex> lock(m);
            result = doUnionAll(result, tmpResult);
        }
    };
    parallelMap(0, exprs.size(), mainWork);

    return result;
}


/*****************************************************************************/
/* THEN EXPRESSION                                                           */
/*****************************************************************************/

ThenExpression::
ThenExpression(const vector<BoolExprPtr> & exprs)
    : CompoundExpression("THEN", exprs)
{
    for (auto e: exprs)
        if (!e->canGenerate())
            throw MLDB::Exception("Cannot have [" + e->print() + "] in a THEN");
}

std::vector<std::pair<SH, Date> >
ThenExpression::
generate(const BehaviorWrapper & behs, SH maxSubject) const
{
    vector<std::pair<SH, Date> > result;

    int done = -1;
    for (unsigned i = 0;  i < exprs.size();  ++i) {
        if (exprs[i]->canGenerate()) {
            result = exprs[i]->generate(behs, maxSubject);
            done = i;
            break;
        }
    }

    for (unsigned i = 0;  i < exprs.size();  ++i) {
        if (i == done) continue;
        result = exprs[i]->filter_after(behs, result);
    }
    
    return result;
}

std::vector<std::pair<SH, Date> >
ThenExpression::
generateAllTimestamps(const BehaviorWrapper & behs, SH maxSubject) const
{
    throw MLDB::Exception("ThenExpression::generateAllTimestamps(): "
                        "not done");
}

std::vector<std::pair<SH, Date> >
ThenExpression::
filter_after(const BehaviorWrapper & behs,
       const std::vector<std::pair<SH, Date> > & input) const
{
    vector<std::pair<SH, Date> > result;
    for (unsigned i = 0;  i < exprs.size();  ++i) {
        result = exprs[i]->filter_after(behs, result);
    }

    return result;
}

std::string
CompoundExpression::
print_(bool isSql) const
{
    string result;
    if (exprs.size() > 0) {
        result = "(";
        for (unsigned i = 0;  i < exprs.size();  ++i) {
            if (i > 0) {
                result += " " + separator + " ";
            }
            result += isSql ? exprs[i]->printSql() : exprs[i]->print();
        }
        result += ")";
    }
    return result;
}

BoolExprPtr 
CompoundExpression::
bind(const BehaviorWrapper & behs, const boost::any & md) const
{
    auto ce = static_pointer_cast<CompoundExpression>(makeCopy());
    ce->exprs.clear();
    for (int i = 0; i < exprs.size(); ++i) {
        auto be = exprs[i]->bind(behs, md);
        if (be) {
            ce->exprs.emplace_back(move(be));
        }
    }
    return ce;
}

/*****************************************************************************/
/* WITHIN EXPRESSION                                                         */
/*****************************************************************************/

WithinExpression::
WithinExpression(const vector<BoolExprPtr> & exprs, 
                 const vector<double>& within_secs)
    : CompoundExpression("WITHIN", exprs), within_secs(within_secs)
{
    for (auto e: exprs)
        if (!e->canGenerate())
            throw MLDB::Exception("Cannot have [" + e->print() + "] in a WITHIN");
}

std::string
WithinExpression::
print()
    const
{
    string result = "(";
    for (unsigned i = 0;  i < exprs.size();  ++i) {
        result += exprs[i]->print();
        if (i != exprs.size() - 1)
            result += " WITHIN{" + 
                boost::lexical_cast<string>(within_secs[i]) +
                ",SECONDS} ";
    }
    result += ")";
    return result;
}

std::string
WithinExpression::
printSql() const
{
    throw MLDB::Exception("Not supported");
}

std::vector<std::pair<SH, Date> >
WithinExpression::
generate(const BehaviorWrapper & behs, SH maxSubject) const
{
    vector<std::pair<SH, Date> > result;

    int done = -1;
    for (unsigned i = 0;  i < exprs.size();  ++i) {
        if (exprs[i]->canGenerate()) {
            result = exprs[i]->generate(behs, maxSubject);
            done = i;
            break;
        }
    }

    for (unsigned i = 0;  i < exprs.size();  ++i) {
        if (i == done) continue;
        result = exprs[i]->filter_after_within(behs, result, 
                within_secs[std::max((unsigned)0, i-1)]);
    }
    
    return result;
}

std::vector<std::pair<SH, Date> >
WithinExpression::
generateAllTimestamps(const BehaviorWrapper & behs, SH maxSubject) const
{
    throw MLDB::Exception("WithinExpression::generateAllTimestamps(): "
                        "not done");
}

std::vector<std::pair<SH, Date> >
WithinExpression::
filter_after_within(const BehaviorWrapper & behs,
       const std::vector<std::pair<SH, Date> > & input,
       unsigned within) const
{
    vector<std::pair<SH, Date> > result;
    for (unsigned i = 0;  i < exprs.size();  ++i) {
        result = exprs[i]->filter_after_within(behs, result, within);
    }

    return result;
}


} // namespace MLDB
