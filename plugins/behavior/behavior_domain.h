/* behavior_domain.h                                              -*- C++ -*-
   Jeremy Barnes, 3 April 2012
   Copyright (c) 2012 mldb.ai inc.  All rights reserved.

   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
*/

#pragma once

#include "mldb/ext/jsoncpp/json.h"
#include "mldb/compiler/compiler.h"
#include <vector>
#include <iostream>
#include "behavior_types.h"
#include "mldb/plugins/behavior/id.h"
#include <functional>
#include "mldb/jml/db/persistent.h"

#include <boost/regex.hpp>
#include "mldb/types/hash_wrapper.h"

#include "mldb/core/dataset.h"


namespace MLDB {

struct PathElement;

/** Stats about a subject in the behavior DB.
*/
struct SubjectStats {
    SubjectStats()
        : earliest(Date::positiveInfinity()),
          latest(Date::negativeInfinity()),
          numBehaviors(0),
          numDistinctBehaviors(0),
          numDistinctTimestamps(0)
    {
    }

    Id id;
    Date earliest;
    Date latest;
    int32_t numBehaviors;
    int32_t numDistinctBehaviors;
    int32_t numDistinctTimestamps;
};

/** Filter for subjects */
struct SubjectFilter {
    SubjectFilter();
    
    /** Does the filter pass the given subject with the given stats? */
    bool pass(SH subject, const SubjectStats & stats) const;

    /** Does the filter fail the given subject, based only upon the
        information not in the stats?
    */
    bool fail(SH subject) const;

    /** Does the filter pass all subjects unconditionally? */
    bool passAll() const;

    /** Tell the filter to only pass subjects whose ID is in the given
        list.
    */
    template<typename Container>
    void filterInList(const Container & list, bool allowEmptyList = false)
    {
        inList.clear();
        inList.insert(list.begin(), list.end());
        theList.clear();
        theList.insert(theList.end(), list.begin(), list.end());
        hasFilterList = true;

        if (!allowEmptyList && theList.empty())
            throw MLDB::Exception("Attempt to filter with empty list");
    }

    bool hasList() const
    {
        return !theList.empty();
    }

    std::vector<SH> getList() const
    {
        return theList;
    }

    /** Tell the filter to only pass subjects whose ID matches the given
        regular expression.
    */
    void filterByRegex(const std::string & expression);

    /** Calculate which stats fields are required in order for the filter to
        function properly.

        This is an or of the SS_* constants in BehaviorDomain.
    */
    int requiredStatsFields() const;

    int minDistinctBehaviors;
    SH minSubject;
    SH maxSubject;
    
private:
    friend class BehaviorDomain;
    std::string includeRegexStr;
    std::unique_ptr<boost::regex> includeRegex;
    Lightweight_Hash_Set<SH> inList;
    bool hasFilterList;
    std::vector<SH> theList;
};

/** Statistics and information about a behavior. */
struct BehaviorStats {
    BehaviorStats()
        : unused(0), subjectCount(0),
          earliest(Date::positiveInfinity()),
          latest(Date::negativeInfinity())
    {
    }
    
    Id       id;           ///< Full ID used in the behaviors
    uint64_t unused;       ///< Used to be count
    uint32_t subjectCount; ///< Number of subjects with this behavior
    Date earliest, latest; ///< Earliest and latest times seen
    
    std::pair<Date, Date> timeRange() const
    {
        return std::make_pair(earliest, latest);
    }
};


/** Filter for behaviors when querying for a list of them. */
struct BehaviorFilter {
    BehaviorFilter();

    /** Does this behavior fail without knowing its stats? */
    bool fail(BH beh) const;

    /** Does this behavior with the given stats pass the filter? */
    bool pass(BH beh, const BehaviorStats & stats) const;

    /** Will all behaviors pass irrespective of the stats? */
    bool passAll() const;

    /** Tell the filter to only pass the behaviors in the given list.
     */
    template<typename Container>
    void filterInList(const Container & list, bool allowEmptyList = false)
    {
        inList.clear();
        inList.insert(list.begin(), list.end());
        hasFilterList = true;

        if (!allowEmptyList && inList.empty())
            throw MLDB::Exception("Attempt to filter with empty list");
    }

    /** Tell the filter to only pass behaviors whose ID matches the given
        regular expression.
    */
    void filterByRegex(const std::string & expression);

    /** Tell the filter to only pass behaviors who have at least the given
        number of distinct subjects.
    */
    void filterByMinSubjectCount(int minCount);

    /** Calculate which stats fields are required in order for the filter to
        function properly.

        This is an or of the BS_* constants in BehaviorDomain.
    */
    int requiredStatsFields() const;

private:
    friend class BehaviorDomain;
    std::string includeRegexStr;
    std::unique_ptr<boost::regex> includeRegex;
    Lightweight_Hash_Set<BH> inList;
    bool hasFilterList;
    int minSubjectCount;
};

struct SubjectIterInfo {
    SubjectIterInfo()
        : index(-1)
    {
    }

    int index;  ///< Unique index for this subject in the order of iteration
};

struct BehaviorIterInfo {
    BehaviorIterInfo()
        : index(-1)
    {
    }

    int index;  ///< Unique index for this behavior in the order of iteration
};

/** Filter for events on a subject. */
struct EventFilter {
    EventFilter(Date earliest = Date::negativeInfinity(),
                Date latest = Date::positiveInfinity())
        : hasFilterList(false), earliest(earliest),
          latest(latest)
    {
    }

    bool pass(BH beh, Date ts, uint32_t count) const
    {
        if (!(ts >= earliest && ts < latest))
            return false;
        if (hasFilterList && !behs.count(beh))
            return false;
        return true;
    }

    bool passAll() const
    {
        return (!hasFilterList
                && earliest == Date::negativeInfinity()
                && latest == Date::positiveInfinity());
    }

    template<typename Container>
    void filterBehInList(const Container & list, bool allowEmptyList = false)
    {
        behs.clear();
        behs.insert(list.begin(), list.end());
        hasFilterList = true;

        if (!allowEmptyList && behs.empty())
            throw MLDB::Exception("Attempt to filter events with empty beh list");
    }

    Lightweight_Hash_Set<BH> behs;
    bool hasFilterList;
    Date earliest;
    Date latest;
};

enum Precision {
    APPROXIMATE,
    EXACT
};

enum Order {
    INORDER,   // In order of timestamps with counts aggregated
    ANYORDER   // Any order, with counts not necessarily aggregated
};

typedef Lightweight_Hash<BH, uint32_t> BehaviorCounts;

enum Parallelism {
    PARALLEL,      ///< Parallelized across threads, in any order
    SERIAL,        ///< Operations occur in serial, but in any order
    ORDERED        ///< Operations occur in serial in strict order
};


/*****************************************************************************/
/* BEHAVIOR DOMAIN                                                          */
/*****************************************************************************/

/** This structure is the interface used to access behaviors.  It is an
    abstract class, that is implemented by a whole family tree of specific
    implementations.
*/

struct BehaviorDomain {
    BehaviorDomain(int minSubjects = 1, double timeQuantum = 1.0,
                    bool hasSubjectIds = true)
        : minSubjects(minSubjects), timeQuantum(timeQuantum),
          hasSubjectIds(hasSubjectIds)
    {
    }

    /// Stream of subject hashes, used to allow generation one at a time
    struct SubjectStream { 
        virtual ~SubjectStream()
        {
        }

        virtual Id next() = 0;       

        virtual Id current() const = 0;

        virtual void advance() = 0;

        virtual void advanceBy(size_t n) = 0;
    };

    /// Stream of behavior hashes, used to allow generation one at a time
    struct BehaviorStream { 
        virtual ~BehaviorStream()
        {
        }

        virtual Id next() = 0;       

        virtual Id current() const = 0;

        virtual void advance() = 0;

        virtual void advanceBy(size_t n) = 0;
    };

    static uint64_t magicStr(const std::string & s);

    typedef MLDB::BehaviorStats BehaviorStats;
    typedef MLDB::SubjectStats SubjectStats;

    virtual ~BehaviorDomain() {}

    /** Make a copy of the data structure.  */
    virtual BehaviorDomain * makeShallowCopy() const = 0;

    /** Make a deep of the data structure. */
    virtual BehaviorDomain * makeDeepCopy() const = 0;
    /** Is there any actual information in the data structure? */
    virtual bool empty() const
    {
        return behaviorCount() == 0;
    }

    /*************************************************************************/
    /* VERB INTERFACE                                                        */
    /*************************************************************************/

    /** Each behavior is the following tuple:

        (subject, verb, object, time, count, metadata)

        for example

        (uid 455252534) (browsed) (pageid 29948589) (at 29 Nov 2012 at 10:13:04am) (once) (with FireFox)

        The verb is a pre-registered id that tells about the kind of event
        that this records.  It is assumed that there are few verbs and
        they are known ahead of time.

        This interface allows the verbs to be modified and controlled.
    */
    
    typedef bool OnVerbFn (VI index, Id id);
    typedef std::function<OnVerbFn> OnVerb;

#if 0
    /** Iterate through all verbs, calling the given callback function.  If
        the callback returns false then iteration will stop.
    */
    virtual bool forEachVerb(const OnVerb & onVerb) const;
    
    /** Register a new verb.  This is a mutating operation; default will throw.
        Returns the structure that should be used when adding it.
    */
    virtual VI addVerb(const Id & id)
    {
        throw MLDB::Exception("not a mutable BehaviorDomain derivation");
    }

    /** Get the index for a verb. */
    virtual VI getVerbIndex(const VH & verb) const;
#endif


    /** How many distinct subjects are known? */
    virtual size_t subjectCount() const = 0;
    
    /** How many distinct behaviors are known? */
    virtual size_t behaviorCount() const = 0;

    /** Return a list of behaviors. */
    virtual std::vector<BH> allBehaviorHashes(bool sorted = false) const = 0;

    /** Return a list of subjects. */
    virtual std::vector<SH>
    allSubjectHashes(SH maxSubject = SH(-1), bool sorted = false) const = 0;

    /** Return a list of subjects with their earliest behavior. */
    virtual std::vector<std::pair<SH, Date> >
    allSubjectsAndEarliestBehavior(SH maxSubject = SH(-1),
                                    bool sorted = false,
                                    bool includeWithNoBeh = false) const;

    /** Return the subject ID for subject N.  Used to set the maxSubject
        parameter based upon an absolute number of subjects.

        Default implementation calls allSubjectHashes and looks up element n.
    */
    virtual SH getMaxSubjectForLimit(int limit) const;

    typedef bool OnSubjectFn (SH, const SubjectIterInfo &);
    typedef std::function<OnSubjectFn> OnSubject;

    /** Iterate over every subject matching the filter and call the given
        callback.
    */
    virtual bool
    forEachSubject(const OnSubject & onSubject,
                   const SubjectFilter & filter = SubjectFilter()) const = 0;

    typedef bool OnSubjectStatsFn (SH, const SubjectIterInfo &, const SubjectStats &);
    typedef std::function<OnSubjectStatsFn> OnSubjectStats;

    enum {
        SS_EARLIEST = 1 << 0,
        SS_LATEST = 1 << 1,
        SS_BEHAVIORS = 1 << 2,
        SS_DISTINCT_BEHAVIORS = 1 << 3,
        SS_DISTINCT_TIMESTAMPS = 1 << 4,
        SS_ID = 1 << 5,   ///< ID of subject

        SS_NONE = 0,
        SS_TIMES = SS_EARLIEST | SS_LATEST,
        SS_ALL = SS_TIMES | SS_BEHAVIORS | SS_DISTINCT_BEHAVIORS | SS_DISTINCT_TIMESTAMPS
                          | SS_ID
        
    };

    /** Iterate over every subject matching the filter and call the given
        callback.  Can happen in parallel.
        
        Default is implemented by allSubjectHashes or forEachSubject depending
        upon parallelism.
    */
    virtual bool
    forEachSubjectParallel(const OnSubjectStats & onSubject,
                           const SubjectFilter & filter = SubjectFilter(),
                           int statsFields = 0,
                           Parallelism parallelism = PARALLEL) const;


    /** Do we know about this subject? */
    virtual bool knownSubject(SH subjectHash) const = 0;

    typedef bool OnBehaviorFn (BH, const BehaviorIterInfo &);
    typedef std::function<OnBehaviorFn> OnBehavior;

    /** Iterate over every behavior matching the filter and call the given
        callback.
    */
    virtual bool
    forEachBehavior(const OnBehavior & onBehavior,
                     const BehaviorFilter & filter = BehaviorFilter()) const;

    /** Return a stream of all behaviors. */
    virtual std::unique_ptr<BehaviorStream>
    getBehaviorStream(size_t start) const = 0;

    /** Return a stream of all subjects. */
    virtual std::unique_ptr<SubjectStream>
    getSubjectStream(size_t start) const = 0;

    typedef bool OnBehaviorStatsFn (BH, const BehaviorIterInfo &, const BehaviorStats &);
    typedef std::function<OnBehaviorStatsFn> OnBehaviorStats;

    /** Iterate over every behavior matching the filter and call the given
        callback.

        The behavior stats will be filled in with at least the given fields.
    */
    virtual bool
    forEachBehaviorParallel(const OnBehaviorStats & onBehavior,
                             const BehaviorFilter & filter = ALL_BEHAVIORS,
                             int statsFields = 0,
                             Parallelism parallelism = PARALLEL) const;


    typedef bool OnBehaviorSubjectsFn (BH,
                                        const BehaviorIterInfo,
                                        const BehaviorStats &,
                                        const std::vector<SH> &);

    typedef std::function<OnBehaviorSubjectsFn> OnBehaviorSubjects;

    /** Iterate over every behavior matching the filter and call the given
        callback with the list of subjects that have the given behavior.

        The behavior stats will be filled in with at least the given fields.

        The returned subjects will be filtered with the given subject filter.
    */
    virtual bool
    forEachBehaviorGetSubjects(const OnBehaviorSubjects & onBehavior,
                                const BehaviorFilter & filter = ALL_BEHAVIORS,
                                int statsFields = 0,
                                const SubjectFilter & subjects = ALL_SUBJECTS,
                                Order order = ANYORDER,
                                Parallelism parallelism = PARALLEL) const;

    
    /** Do we know about this behavior? */
    virtual bool knownBehavior(BH behHash) const = 0;

    /** How many distinct timestamps does this user have?  Stop counting
        at the given max value.
    */
    virtual int
    numDistinctTimestamps(SH subjectHash, uint32_t maxValue = -1) const = 0;

    virtual SubjectStats
    getSubjectStats(SH subjectHash,
                    bool needBehaviorCount = true,
                    bool needTimestampCount = false) const = 0;

    /** Return only the given fields (SS_ constants) for the given subject. */
    virtual SubjectStats
    getSubjectStatsFields(SH subjectHash, int fields = SS_NONE) const;

    virtual std::pair<Date, Date>
    getSubjectTimestampRange(SH subjectHash) const = 0;

    /** For the given behavior, return all subjects. */
    virtual std::vector<SH>
    getSubjectHashes(BH beh, SH maxSubject = SH(-1), bool sorted = false)
        const = 0;

    std::vector<SH>
    getSubjectHashesFromHash(BH beh, SH maxSubject = SH(-1),
                             bool sorted = false) const
    {
        return getSubjectHashes(beh, maxSubject, sorted);
    }

    /** For the given behavior, return all subjects and the earliest
        time that the behavior occurred.
    */
    virtual std::vector<std::pair<SH, Date> >
    getSubjectHashesAndTimestamps(BH beh, SH maxSubject = SH(-1),
                                  bool sorted = false) const = 0;
    
    /** For the given behavior, return all subjects and all timestamps
        at which the behavior occurred for the subjects.

        Default implementation calls getSubjectHashesAndTimestamps and
        then iterates through each subject ID found, looking for other
        instances of the given behavior.
    */
    virtual std::vector<std::pair<SH, Date> >
    getSubjectHashesAndAllTimestamps(BH beh, SH maxSubject = SH(-1),
                                     bool sorted = false) const;
    
    std::vector<std::pair<SH, Date> >
    getSubjectHashesAndTimestampsFromHash(BH beh, SH maxSubject = SH(-1),
                                          bool sorted = false) const
    {
        return getSubjectHashesAndTimestamps(beh, maxSubject, sorted);
    }

    typedef bool OnBehaviorSubjectFn (SH subject, Date earliestTs);
    typedef std::function<OnBehaviorSubjectFn> OnBehaviorSubject;

    /** For the given behavior, calls the callback with each subject who
        had that behavior and optionally the earliest timestamp on
        which the behavior was recorded.
    */
    virtual bool
    forEachBehaviorSubject(BH beh,
                            const OnBehaviorSubject & onSubject,
                            bool withTimestamps = false,
                            Order order = INORDER,
                            SH maxSubject = SH::max()) const;

    virtual Id getSubjectId(SH subjectHash) const = 0;

    virtual SH subjectIdToHash(const Id & id) const
    {
        return SH(id.hash());
    }

    virtual Id getBehaviorId(BH behHash) const = 0;

    virtual BH behaviorIdToHash(const Id & id) const
    {
        return BH(id.hash());
    }

    Id getBehaviorIdFromHash(BH behHash) const
    {
        return getBehaviorId(behHash);
    }

    virtual std::pair<Date, Date>
    getBehaviorTimeRange(BH beh) const
    {
        return getBehaviorStats(beh, BS_TIMES).timeRange();
    }

    std::pair<Date, Date>
    getBehaviorTimeRangeFromHash(BH beh) const
    {
        return getBehaviorTimeRange(beh);
    }

    virtual size_t
    getBehaviorSubjectCount(BH beh, SH maxSubject = SH::max(),
                             Precision p = EXACT) const
    {
        if (maxSubject.isMax())
            return getBehaviorStats(beh, BS_SUBJECT_COUNT).subjectCount;
        return getSubjectHashes(beh, maxSubject).size();
    }

    enum {
        BS_SUBJECT_COUNT = 1 << 1,
        BS_EARLIEST      = 1 << 2,
        BS_LATEST        = 1 << 3,
        BS_ID            = 1 << 4,

        BS_NONE          = 0,
        BS_TIMES         = BS_EARLIEST | BS_LATEST,
        BS_ALL           = BS_SUBJECT_COUNT | BS_EARLIEST | BS_LATEST | BS_ID
    };

    virtual BehaviorStats
    getBehaviorStats(BH behavior, int fields /*= BS_ALL*/) const = 0;

    /** Calculate a behavior similarity reduction.  This score will
        arrange for iteration over each subject that has been
        recorded in either or both behaviors.
    */
    typedef std::function<bool (SH subject)> OnBehaviors;

    virtual int coIterateBehaviors(BH beh1, BH beh2, 
                                    SH maxSubject = SH::max(),
                                    const OnBehaviors & onBehaviors
                                        = OnBehaviors()) const;

    // Implementation of the above
    template<typename Beh>
    int coIterateBehaviorsImpl(Beh beh1, Beh beh2, 
                                SH maxSubject,
                                const OnBehaviors & onBehaviors) const;

    /** Return if the subject has at least n distinct behaviors. */
    virtual bool subjectHasNDistinctBehaviors(SH subject, int N) const;

    virtual std::vector<std::pair<BH, uint32_t> >
    getSubjectBehaviorCounts(SH subjectHash, Order order = INORDER) const = 0;

    virtual std::vector<uint32_t>
    getSubjectBehaviorCounts(SH subjectHash,
                              const std::vector<BH> & behs,
                              uint32_t maxCount = -1) const;


    typedef EventFilter SubjectBehaviorFilter;
    typedef MLDB::SubjectFilter SubjectFilter;
    typedef MLDB::BehaviorFilter BehaviorFilter;
    typedef MLDB::SubjectIterInfo SubjectIterInfo;
    typedef MLDB::BehaviorIterInfo BehaviorIterInfo;

    static const SubjectFilter ALL_SUBJECTS;
    static const EventFilter ALL_EVENTS;
    static const BehaviorFilter ALL_BEHAVIORS;

    /** getSubjectBehaviorCounts, but including a filter on the events.  Implemented using
        forEachSubjectBehaviorHash.
    */
    virtual std::vector<std::pair<BH, uint32_t> >
    getSubjectBehaviorCountsFiltered(SH subj,
                                      const EventFilter & filter,
                                      Order order = INORDER) const;


    /** Compound query of the type

        SELECT DISTINCT beh, count(beh)
        FROM EVENTS
        WHERE (event filter)
        AND subject IN
            SELECT DISTINCT subject FROM EVENTS
            WHERE (subject filter)
        SORT BY beh (only when resultOrder == INORDER)
    */

    virtual bool forEachSubjectGetBehaviorCounts
        (const std::function<bool (SH subject,
                                   const SubjectIterInfo & info,
                                   const SubjectStats & stats,
                                   const std::vector<std::pair<BH, uint32_t> > & counts) > & onSubject,
         const SubjectFilter & subjectFilter,
         EventFilter eventFilter,
         Order resultOrder = ANYORDER,
         int statsFields = SS_NONE,
         Parallelism inParallel = PARALLEL) const;

    /** Return all matching events for each matching subject. */

    virtual bool forEachSubjectGetEvents
        (const std::function<bool (SH subject,
                                   const SubjectIterInfo & info,
                                   const SubjectStats & stats,
                                   const std::vector<std::tuple<BH, Date, uint32_t> > & counts) > & onSubject,
         const SubjectFilter & subjectFilter,
         EventFilter eventFilter,
         Order resultOrder = ANYORDER,
         int statsFields = SS_NONE,
         Parallelism inParallel = PARALLEL) const;

    /** Return all timestamps for each matching subject. */
    
    virtual bool forEachSubjectGetTimestamps
        (const std::function<bool (SH subject,
                                   const SubjectIterInfo & info,
                                   const SubjectStats & stats,
                                   const std::vector<std::pair<Date, Lightweight_Hash<BH, int> > > & onSubject) > & onSubject,
         const SubjectFilter & subjectFilter,
         EventFilter eventFilter,
         Order resultOrder = ANYORDER,
         int statsFields = SS_NONE,
         Parallelism inParallel = PARALLEL) const;

    uint32_t getSubjectBehaviorCount(SH subjectHash, BH beh,
                                      uint32_t maxCount = -1) const;

    // NOTE: the BehaviorDomain interface doesn't provide an interface that
    // uses this TypeDef, but several derived classes do as internally
    // behaviors are stored in-order.
    typedef bool OnSubjectBehaviorIndexFn (BI beh, Date ts, uint32_t count);
    typedef std::function<OnSubjectBehaviorIndexFn> OnSubjectBehaviorIndex;

    typedef bool OnSubjectBehaviorHashFn (BH beh, Date ts, uint32_t count);
    typedef std::function<OnSubjectBehaviorHashFn> OnSubjectBehaviorHash;

    virtual bool forEachSubjectBehaviorHash
        (SH subject,
         const OnSubjectBehaviorHash & onBeh,
         EventFilter filter = EventFilter(),
         Order order = INORDER) const = 0;

    virtual bool forEachSubjectBehaviorHashBackwards
        (SH subject,
         const OnSubjectBehaviorHash & onBeh,
         EventFilter filter = EventFilter(),
         Order order = INORDER) const;

    bool forEachSubjectBehavior(SH subject,
                                 const OnSubjectBehaviorHash & onBeh,
                                 EventFilter filter = EventFilter(),
                                 Order order = INORDER) const
    {
        return forEachSubjectBehaviorHash(subject, onBeh, filter, order);
    }

    enum Direction {
        FORWARDS,
        BACKWARDS
    };

    /** Function used to tell what we do per timestamp for a given subject. */
    typedef std::function<bool (Date, const Lightweight_Hash<BH, int> &)>
        OnSubjectTimestampMap;

    /** Call the given callback on each individual timestamp for the given
        subject.
    */
    virtual bool
    forEachSubjectTimestamp(SH subject,
                            const OnSubjectTimestampMap & onTs,
                            Direction direction = FORWARDS,
                            Date earliest = Date::negativeInfinity(),
                            Date latest = Date::positiveInfinity()) const;

    struct SubjectBehavior {
        SubjectBehavior(BH beh = BH(), Date date = Date::notADate(), int count = 0)
            : beh(beh), date(date), count(count)
        {
        }

        BH beh;
        Date date;
        int count;

        bool operator == (const SubjectBehavior & other) const
        {
            return beh == other.beh && date == other.date && count == other.count;
        }

        bool operator != (const SubjectBehavior & other) const
        {
            return ! operator == (other);
        }
    };

    /** Return all events that occurred for the given subject on the given
        timestamp.
    */
    virtual Lightweight_Hash<BH, int>
    getSubjectTimestamp(SH subject, Date ts) const;

    /** Return the entire set of behaviors for the given subject. */
    virtual std::vector<SubjectBehavior>
    getSubjectBehaviors(SH subject,
                         EventFilter filter = EventFilter(),
                         Order order = INORDER) const;

    typedef std::function<void (Date & earliest, Date & conversion)> FixDate;

    /** Insert the behavior counts for those that occur before the
        given date into the stats data structure.  Used for unbiased
        behavior updates.

        The converters set contains a set of behaviors that are considered
        to be a conversion event.  The algorithm will scan through the
        behaviors until it finds the ealiest of these, and use that as
        the conversion date.

        Once a conversion date is obtained, the fixDate function will be
        called to determine an effective conversion date.  This can be
        used to (for example) ignore events in a fixed window before
        conversion.  If no function is supplied, then the identity
        function will be called.

        The ignored set contains a set of behaviors to be ignored.  These
        will simply be passed over.

        The earliestDate parameter gives a date before which no events
        will be included.
    */
    virtual Date updateUnbiasedSubjectBehaviorCounts
        (SH subject,
         const Lightweight_Hash_Set<BH> & converters,
         const Lightweight_Hash_Set<BH> & ignored,
         BehaviorCounts & counts,
         Date earliestDate = Date::negativeInfinity(),
         const FixDate & fixDate = FixDate()) const;

    /** Same as above, but with a fixed date. */
    virtual BehaviorCounts
    getUnbiasedSubjectBehaviorCountsFixedDate
        (SH subject,
         const Lightweight_Hash_Set<BH> & converters,
         const Lightweight_Hash_Set<BH> & ignored,
         BehaviorCounts & counts,
         Date earliestDate = Date::negativeInfinity(),
         Date latestDate = Date::positiveInfinity()) const;

    /** Return a list of subjects that have at least n distinct
        behaviors. */
    virtual std::vector<SH> subjectsWithNBehaviors(int n) const;

    /** Return the subjectID of all subjects in PathElement */
    std::vector<MLDB::Path> allSubjectPath(ssize_t start = 0, ssize_t limit = -1) const;


    /*************************************************************************/
    /* METADATA                                                              */
    /*************************************************************************/

    /** This is a basic JSON-based metadata mechanism that allows extra
        metadata to be recorded along with the file.
    */

    /** Return the actual earliest time in the file. */
    virtual Date earliestTime() const = 0;

    /** Return the actual latest time in the file. */
    virtual Date latestTime() const = 0;

    /** Return the nominal start time in the file. */
    virtual Date nominalStart() const = 0;

    /** Return the nominal end time in the file. */
    virtual Date nominalEnd() const = 0;

    /** Return the total number of recorded behavior events.  Returns -1
        if it's not known.
    */
    virtual int64_t totalEventsRecorded() const
    {
        return -1;
    }

    /** Return the metadata for the specified key. */
    virtual Json::Value getFileMetadata(const std::string & key) const = 0;

    /** Return whether the metadata exists or not. */
    virtual bool fileMetadataExists(const std::string & key) const = 0;

    /** Set the specified metadata information. */
    virtual void setFileMetadata(const std::string & key,
                                 Json::Value && value)
    {
        throw MLDB::Exception("This BehaviorDomain implementation doesn't "
                            "support metadata");
    }

    /** Get all metadata for all keys and return in the Json::Value */
    virtual Json::Value getAllFileMetadata() const = 0;

    /** Returns the approximate total memory usage in bytes of the
        data structure.
    */
    virtual int64_t approximateMemoryUsage() const
    {
        return -1;
    }

    /*************************************************************************/
    /* MUTATING OPERATIONS                                                   */
    /*************************************************************************/

    virtual void
    record(const Id & subject,
           const Id & behavior, Date ts,
           uint32_t count = 1, const Id & verb = Id())
    {
        throw MLDB::Exception("record(): not implemented for this type");
    }

    void
    recordId(const Id & subject, const Id & behavior, Date ts,
             uint32_t count = 1, const Id & verb = Id())
    {
        return record(subject, behavior, ts, count);
    }

    virtual void
    recordOnce(const Id & subject,
               const Id & behavior, Date ts, const Id & verb = Id())
    {
        throw MLDB::Exception("recordOnce(): not implemented for this type");
    }

    void
    recordOnceId(const Id & subject, const Id & behavior,
                 Date ts, const Id & verb = Id())
    {
        return recordOnce(subject, behavior, ts, verb);
    }

    virtual void setNominalTimeRange(Date start, Date end)
    {
        throw MLDB::Exception("setNominalTimeRange(): "
                            "not implemented for this type");
    }

    /** Once all mutation has been done, finish off anything that needs
        to be done.  Most classes will have a nop for this.
    */
    virtual void finish()
    {
    }

    /** Add another domain to the current one. */
    virtual void merge(std::shared_ptr<BehaviorDomain> other)
    {
        throw MLDB::Exception("BehaviorDomain::merge(): "
                            "not implemented for this type");
    }

    /** Dump the contents of the behaviors to the given stream. */
    void dump(std::ostream & stream) const;
    
    /** Dump everything known about the given subject to the given stream. */
    void dumpSubject(SH subject, std::ostream & stream = std::cerr) const;

    /** Print out some statistics to stderr */
    void stats(std::ostream & stream = std::cerr) const;

    /** Verify the internal integrity. */
    void verify();

    enum {
        KEEP_ALL_BEHAVIORS = -1
    };

    void save(const std::string & filename,
              ssize_t maxSubjectBehaviors = KEEP_ALL_BEHAVIORS);
    void saveToStream(std::ostream & stream,
                      ssize_t maxSubjectBehaviors = KEEP_ALL_BEHAVIORS);

    uint64_t serialize(ML::DB::Store_Writer & store,
                       ssize_t maxSubjectBehaviors = KEEP_ALL_BEHAVIORS);

    Date unQuantizeTime(uint64_t tm) const
    {
        return Date::fromSecondsSinceEpoch(tm * timeQuantum);
    }

    uint64_t quantizeTime (Date tm) const
    {
        return quantizeTimeStatic(tm, timeQuantum);
    }

    static uint64_t quantizeTimeStatic(Date tm, double timeQuantum)
    {
        tm.quantize(timeQuantum);
        return (uint64_t)(tm.secondsSinceEpoch() / timeQuantum);
    }

    int minSubjects;
    double timeQuantum;
    bool hasSubjectIds;
};

std::ostream &
operator << (std::ostream & stream, const BehaviorDomain::SubjectBehavior & beh);

} // namespace MLDB
