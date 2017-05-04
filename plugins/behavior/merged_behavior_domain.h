/* merged_behavior_domain.h                                      -*- C++ -*-
   Jeremy Barnes, 4 April 2012
   Copyright (c) 2012 mldb.ai inc.  All rights reserved.

   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
*/

#pragma once


#include "behavior_domain.h"
#include "mldb/jml/utils/lightweight_hash.h"
#include "mldb/builtin/id_hash.h"

namespace MLDB {

extern __thread bool debugMergedBehaviors;



/*****************************************************************************/
/* MERGED BEHAVIOR DOMAIN                                                   */
/*****************************************************************************/

/** Get the merged behaviors. */

struct MergedBehaviorDomain : public BehaviorDomain {

    MergedBehaviorDomain();

    MergedBehaviorDomain(const MergedBehaviorDomain & other);

    MergedBehaviorDomain(const std::vector<std::shared_ptr<BehaviorDomain> >
                          & toMerge, bool preIndex);

    void clear();

#if 0
    // TODO: we could have one which gets recorded into...
    virtual void
    record(const Id & subject, const Id & behavior, Date ts,
           uint32_t count = 1);

    virtual void
    recordOnce(const Id & subject, const Id & behavior, Date ts);

    virtual void finish();
#endif

    void init(std::vector<std::shared_ptr<BehaviorDomain> > toMerge,
              bool preIndex);

    /** Make a copy of the data structure.  */
    virtual MergedBehaviorDomain * makeShallowCopy() const;

    /** Make a deep of the data structure. */
    virtual MergedBehaviorDomain * makeDeepCopy() const;

    /** How many distinct subjects are known? */
    virtual size_t subjectCount() const;
    
    /** How many distinct behaviors are known? */
    virtual size_t behaviorCount() const;

    virtual std::unique_ptr<BehaviorStream>
    getBehaviorStream(size_t start) const;

    virtual std::unique_ptr<SubjectStream>
    getSubjectStream(size_t start) const;

    /** Return a list of behaviors. */
    virtual std::vector<BH> allBehaviorHashes(bool sorted = false) const;

    /** Return a list of subjects. */
    virtual std::vector<SH>
    allSubjectHashes(SH maxSubject = SH(-1), bool sorted = false) const;

    virtual bool
    forEachSubject(const OnSubject & onSubject,
                   const SubjectFilter & filter = SubjectFilter()) const;

    virtual bool knownSubject(SH subjectHash) const;

    virtual bool knownBehavior(BH behHash) const;

    virtual int coIterateBehaviors(BH beh1, BH beh2, 
                                    SH maxSubject = (SH)-1,
                                    const OnBehaviors & onBehaviors
                                        = OnBehaviors()) const;

    /** Return if the subject has at least n distinct behaviors. */
    virtual bool subjectHasNDistinctBehaviors(SH subject, int N) const;
    
    /** Return behavior info for the given hash. */
    //virtual SubjectInfo
    //getSubjectInfo(SH subjectHash) const;

    virtual SubjectStats
    getSubjectStats(SH subjectHash,
                    bool needDistinctBehaviors = true,
                    bool needDistinctTimestamps = false) const;

    virtual std::pair<Date, Date>
    getSubjectTimestampRange(SH subjectHash) const;

    virtual int
    numDistinctTimestamps(SH subjectHash, uint32_t maxValue = -1) const;

    virtual std::vector<SH>
    getSubjectHashes(BH beh, SH maxSubject = SH(-1), bool sorted = false)
        const;

    virtual std::vector<std::pair<SH, Date> >
    getSubjectHashesAndTimestamps(BH beh, SH maxSubject = SH(-1),
                                  bool sorted = false) const;

    virtual std::vector<std::pair<SH, Date> >
    getSubjectHashesAndAllTimestamps(BH beh, SH maxSubject = SH(-1),
                                     bool sorted = false) const;
    
    virtual bool
    forEachBehaviorSubject(BH beh,
                            const OnBehaviorSubject & onSubject,
                            bool withTimestamps = false,
                            Order order = INORDER,
                            SH maxSubject = SH::max()) const;

    virtual Id getSubjectId(SH subjectHash) const;

    virtual Id getBehaviorId(BH behHash) const;

    virtual BehaviorStats
    getBehaviorStats(BH behavior, int fields) const;

    virtual size_t
    getBehaviorSubjectCount(BH beh, SH maxSubject = SH::max(),
                             Precision p = EXACT) const;

    size_t getApproximateBehaviorSubjectCountImpl(BH beh) const;

    size_t getExactBehaviorSubjectCountImpl(BH beh, SH maxSubject,
                                             Precision p) const;

    virtual std::vector<std::pair<BH, uint32_t> >
    getSubjectBehaviorCounts(SH subjectHash, Order order = INORDER) const;

    virtual bool forEachSubjectBehaviorHash
        (SH subject,
         const OnSubjectBehaviorHash & onBeh,
         SubjectBehaviorFilter filter = SubjectBehaviorFilter(),
         Order order = INORDER) const;

    using BehaviorDomain::getSubjectBehaviorCounts;

    virtual Date earliestTime() const
    {
        return earliest_;
    }

    virtual Date latestTime() const
    {
        return latest_;
    }

    virtual Date nominalStart() const
    {
        return nominalStart_;
    }

    virtual Date nominalEnd() const
    {
        return nominalEnd_;
    }

    virtual bool fileMetadataExists(const std::string & key) const;

    virtual Json::Value getFileMetadata(const std::string & key) const;

    virtual Json::Value getAllFileMetadata() const;

    /** Returns (and/or caches) the bitmap of which entries contain the
        given subject.
    */
    uint32_t getSubjectBitmap(SH subject) const;

    //private:
public:
    std::vector<std::shared_ptr<BehaviorDomain> > behs;

    IdHashes behIndex;
    IdHashes subjectIndex;

    Date earliest_, latest_, nominalStart_, nominalEnd_;

    bool nonOverlappingInTime;

    Json::Value fileMetadata_;
};


} // namespace MLDB
