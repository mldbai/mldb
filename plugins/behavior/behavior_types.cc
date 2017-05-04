// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* behavior_types.cc                                              -*- C++ -*-
   Jeremy Barnes, 5 April 2012
   Copyright (c) 2012 mldb.ai inc.  All rights reserved.

   Types for behaviors.
*/

#include "behavior_types.h"

using namespace std;

namespace MLDB {


/*****************************************************************************/
/* SUBJECT INFO                                                              */
/*****************************************************************************/

bool
SubjectInfo::
recordOnce(uint64_t beh, Date ts, uint32_t count)
{
    int nb = behaviors.size();
    for (int i = 0;  i < nb;  ++i) {
        if (behaviors[i].behavior == beh) {
            behaviors[i].timestamp
                = std::min(behaviors[i].timestamp, ts);
            return false;  // already recorded
        }
    }
        
    record(beh, ts, count);

    return true;
}

void
SubjectInfo::
sort()
{
    if (behaviors.size() < 2) return;
    std::sort(behaviors.begin(), behaviors.end());

#if 0
    for (unsigned i = 0;  i < behaviors.size();  ++i)
        cerr << i << " " << behaviors[i].timestamp << " "
             << behaviors[i].behavior << " " << behaviors[i].count
             << endl;
#endif

    // Look for where two adjacent can be merged because they have the
    // same timestamp.
    int in = 1, out = 0;
    for (;in < behaviors.size(); ++in) {

#if 0
        cerr << "in = " << in << " out = " << out << endl;

        for (unsigned i = 0;  i < behaviors.size();  ++i)
            cerr << i << " " << behaviors[i].timestamp << " "
                 << behaviors[i].behavior << " " << behaviors[i].count
                 << (i == in ? " <=in" : "")
                 << (i == out ? " <=out" : "")
                 << endl;
#endif

        auto & bin = behaviors[in];
        auto & bout = behaviors[out];

        if (bin.timestamp == bout.timestamp
            && bin.behavior == bout.behavior) {
            bout.count += bin.count;
            //cerr << "  *** match" << endl;
        }
        else {
            ++out;
            if (out != in)
                behaviors[out] = bin;
            //cerr << "  *** no match" << endl;
        }
    }

    if (out < behaviors.size() - 1) {
        //cerr << "trimming from " << behaviors.size() << " to "
        //     << out + 1 << endl;
        behaviors.erase(behaviors.begin() + out + 1, behaviors.end());
    }

    //if (!std::is_sorted(behaviors.begin(), behaviors.end()))
    //    throw MLDB::Exception("not sorted");
}

Json::Value
SubjectInfo::
toJson() const
{
    Json::Value result;
    result["firstSeen"] = firstSeen.secondsSinceEpoch();
    if (numDistinctBehaviors != -1)
        result["numDistinctBehaviors"] = numDistinctBehaviors;
    
    Json::Value & behs = result["behaviors"];
    for (unsigned i = 0;  i < behaviors.size();  ++i) {
        Json::Value & beh = behs[i];
        beh[0] = behaviors[i].timestamp.secondsSinceEpoch();
        beh[1] = behaviors[i].behavior;
        
        if (behaviors[i].count > 1)
            beh[2] = behaviors[i].count;
    }
    
    return result;
}

SubjectInfo &
SubjectInfo::
operator += (const SubjectInfo & other)
{
    firstSeen = std::min(firstSeen, other.firstSeen);
    numDistinctBehaviors = -1;
    if (firstSeen < other.firstSeen)
        behaviors.insert(behaviors.end(),
                          other.behaviors.begin(), other.behaviors.end());
    else behaviors.insert(behaviors.begin(),
                           other.behaviors.begin(), other.behaviors.end());
    return *this;
}


/*****************************************************************************/
/* BEHAVIOR INFO                                                            */
/*****************************************************************************/

void
BehaviorInfo::
record(uint64_t sub, Date ts, uint32_t count)
{
    earliest = std::min(earliest, ts);
    latest = std::max(latest, ts);

    //static uint64_t n = 0;

    //uint64_t hopsBefore = Subjects::numHops;

    auto it = subjects.insert(make_pair(sub, ts)).first;
    it->second = min(ts, it->second);

#if 0
    uint64_t hopsAfter = Subjects::numHops;

    uint64_t hopsNeeded = hopsAfter - hopsBefore;

    if (hopsNeeded > 100 && false) {
        cerr << "inserting " << sub << " size = " << subjects.size()
             << " capacity = " << subjects.capacity()
             << " hops = " << hopsNeeded << endl;
    }
#endif

    seen += count;
}

void
BehaviorInfo::
merge(BehaviorInfo && other)
{
    earliest.setMin(other.earliest);
    latest.setMax(other.latest);
    seen += other.seen;
    if (subjects.empty())
        subjects = other.subjects;
    else {
        for (auto it = other.subjects.begin(), end = other.subjects.end();
             it != end;  ++it) {
            auto res = subjects.insert(*it);
            if (!res.second)
                res.first->second.setMin(it->second);
        }
    }
}

void
BehaviorInfo::
merge(const BehaviorInfo & other)
{
    earliest.setMin(other.earliest);
    latest.setMax(other.latest);
    seen += other.seen;
    for (auto it = other.subjects.begin(), end = other.subjects.end();
         it != end;  ++it) {
        auto res = subjects.insert(*it);
        if (!res.second)
            res.first->second.setMin(it->second);
    }
}

void
BehaviorInfo::
sort()
{
#if 0
    std::sort(subjects.begin(), subjects.end());
    subjects.erase(std::unique(subjects.begin(), subjects.end()),
                   subjects.end());
    if (subjects.capacity() > 2 * subjects.size()) {
        Subjects newSubjects(subjects);
        subjects.swap(newSubjects);
    }
#endif
}


} // namespace MLDB
