/* behavior_types.h                                               -*- C++ -*-
   Jeremy Barnes, 4 April 2012
   Copyright (c) 2012 mldb.ai inc.  All rights reserved.

   Types used for behaviors.
*/

#pragma once

#include "mldb/types/date.h"
#include "mldb/arch/exception.h"
#include "mldb/utils/compact_vector.h"
#include "mldb/base/less.h"
#include "mldb/jml/utils/lightweight_hash.h"
#include "mldb/ext/jsoncpp/json.h"

namespace MLDB {

// TODO: get rid of this...

typedef uint64_t SubjectId;
typedef uint64_t BehaviorId;


/*****************************************************************************/
/* SUBJECT BEHAVIOR                                                         */
/*****************************************************************************/

struct SubjectBehavior {
    Date timestamp;
    BehaviorId behavior;
    uint32_t count;

    bool operator < (const SubjectBehavior & other) const
    {
        return MLDB::less_all(timestamp, other.timestamp,
                            behavior, other.behavior,
                            count, other.count);
    }

    bool operator == (const SubjectBehavior & other) const
    {
        return timestamp == other.timestamp
            && behavior == other.behavior
            && count == other.count;
    }
};

inline std::ostream & operator << (std::ostream & stream,
                                   const SubjectBehavior & beh)
{
    return stream << "(" << beh.timestamp << "," << beh.behavior
                  << "," << beh.count << ")";
}

/*****************************************************************************/
/* SUBJECT INFO                                                              */
/*****************************************************************************/

struct SubjectInfo {
    SubjectInfo()
        : firstSeen(Date::positiveInfinity()),
          numDistinctBehaviors(-1)
    {
    }

    Date firstSeen;
    typedef compact_vector<SubjectBehavior, 1> Behaviors;
    Behaviors behaviors;  ///< Actual behaviors
    int numDistinctBehaviors;  ///< -1 if unknown

    bool recordOnce(uint64_t beh, Date ts, uint32_t count);

    void record(uint64_t beh, Date ts, uint32_t count)
    {
        firstSeen = std::min(firstSeen, ts);
        SubjectBehavior sb;
        sb.behavior = beh;
        sb.timestamp = ts;
        sb.count = count;
        behaviors.push_back(sb);
    }

    void sort();

    Json::Value toJson() const;

    SubjectInfo & operator += (const SubjectInfo & other);
};


/*****************************************************************************/
/* BEHAVIOR INFO                                                            */
/*****************************************************************************/

struct BehaviorInfo {
    BehaviorInfo()
        : seen(0),
          earliest(Date::positiveInfinity()),
          latest(Date::negativeInfinity())
    {
    }

    //typedef compact_vector<SubjectId> Subjects;
    // Each subject along with the earliest timestamp for the subject and
    // behavior
    typedef Lightweight_Hash<SubjectId, Date> Subjects;

    Subjects subjects;  ///< Sorted list of all subjects with this behavior
    uint64_t seen;      ///< Total number of times this behavior has been seen
    Date earliest, latest; ///< Earliest and latest time we saw the behavior

    size_t numDistinctSubjects() const { return subjects.size(); }

    //Json::Value meta;   ///< Metadata for the behavior

    void record(uint64_t sub, Date ts, uint32_t count);

    void merge(BehaviorInfo && other);
    void merge(const BehaviorInfo & other);

    void sort();

    void clear()
    {
        seen = 0;
        earliest = Date::positiveInfinity();
        latest = Date::negativeInfinity();
        subjects.clear();
    }
};

} // namespace MLDB
