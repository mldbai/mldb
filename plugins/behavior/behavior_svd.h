// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* behavior_svd.h                                                 -*- C++ -*-
   Jeremy Barnes, 6 May 2012
   Copyright (c) 2012 mldb.ai inc.  All rights reserved.

   SVD functionality for behaviors.
*/

#ifndef __behavior__behavior_svd_h__
#define __behavior__behavior_svd_h__

#include <vector>
#include "mldb/jml/stats/distribution.h"
#include "behavior_domain.h"
#include <map>
#include "mldb/jml/utils/lightweight_hash.h"
#include "mldb/ml/svd_utils.h"


namespace MLDB {


/*****************************************************************************/
/* BEHAVIOR SVD                                                             */
/*****************************************************************************/

struct BehaviorSvd {

    BehaviorSvd()
        : space(HAMMING), calcLongTail(true)
    {
    }

    BehaviorSvd(SH maxSubject,
                 int numDenseBehaviors,
                 int numSingularValues,
                 const std::vector<BH> & biasedBehaviors = std::vector<BH>(),
                 SvdSpace space = HAMMING,
                 bool calcLongTail = true);

    BehaviorSvd(const std::string & filename);

    SH maxSubject;

    int numDenseBehaviors;
    int numSingularValues;
    std::vector<BH> denseBehaviors;
    std::vector<BH> sparseBehaviors;
    Lightweight_Hash_Set<uint64_t> biasedBehaviors;

    SvdSpace space;
    bool calcLongTail;

    /** Singular values */
    distribution<float> singularValues;

    /** Singular vector for every single behavior (dense and sparse) */
    std::vector<distribution<float> > singularVectors;

    /** List of all behaviors */
    std::vector<BH> allBehaviors;

    /** Index of behavior -> index */
    Lightweight_Hash<BH, int> behaviorIndex;

    /** Singular vector for each of the dense behaviors. */
    std::vector<distribution<float> > denseVectors;

    /** Create the SVD. */
    void train(const BehaviorDomain & behs,
               std::function<bool (const Json::Value & progress)> = nullptr);

    void trainNoProgress(const BehaviorDomain & behs)
    {
        train(behs, nullptr);
    }

    /** Partition the list of behaviors into a set of dense and sparse
        behaviors.
    */
    void partitionBehaviors(const BehaviorDomain & behs);

    /** Function used to calculate the overlap between two behaviors.

        Default simply counts the number of users in common, in other
        words it causes the SVD to operate on the *Hamming Space*.

        This is equivalent to using the normal dot product on a
        binary vector.
    */
    float calcOverlap(BH i, BH j, const BehaviorDomain & behs) const;

    /** Given a behavior, recalculate its singular vector.  Normally should
        not be used, in favour of getBehaviorVector.
    */
    distribution<float>
    calculateBehaviorVector(BH beh, const BehaviorDomain & behs) const;

    void serialize(ML::DB::Store_Writer & store) const;
    void reconstitute(ML::DB::Store_Reader & store);

    void save(const std::string & filename) const;
    void load(const std::string & filename);

    bool knownBehaviorHash(BH) const;

    int getNumSingularValues() const;

    /** Dense matrix (diagonal) of overlaps between segments. */
    std::vector<distribution<float> > denseOverlaps;

    /** Run the SVD over the dense behaviors, and extract the singular vectors
        from them.
    */
    void calcDenseSvd();

    distribution<float>
    getBehaviorVector(BH beh) const;

    // Returns a null pointer if it's not in the cache
    const distribution<float> *
    getBehaviorVectorCached(BH beh) const;

    distribution<float>
    getFullBehaviorVector(BH beh) const;

    /** Given a subject, calcule its corresponding singular vector,
        using all of his behaviors */
    distribution<float>
    calculateSubjectVector(SH subjectHash, const BehaviorDomain & behs) const;
    
    /** Given a list of BH and associated count, calculate the corresponding
        singular vector **/
    distribution<float>
    calculateSubjectVectorForBh(const std::vector<std::pair<BH, uint32_t>> & behs) const;

    /** Given a subject and a sparse weighting of its behaviors, calculate
        a corresponding singular vector.
    */
    distribution<float>
    calculateWeightedSubjectVector(const std::vector<std::pair<double, BH> > & behaviors) const;
    

    /** Explain which behaviors contribute the most to a dimension, both
        positive and negative.
    */
    std::pair<std::vector<std::pair<BH, float> >,
              std::vector<std::pair<BH, float> > >
    explainDimension(int dim, int numBehs = 20) const;

    struct BehaviorCache {
        std::vector<BH> behaviors;
        std::vector<SvdColumnEntry> subjects;
        Lightweight_Hash<BH, int> behToIndex;
    };

    void updateBehaviorCache(const BehaviorDomain & behs,
                              SH maxSubject,
                              BehaviorCache & cache) const;

    /** Run over all sparse vectors and call calcBehaviorVector to complete
        the SVD.
    */
    void calcLongTailVectors(const BehaviorDomain & behs,
                             const BehaviorCache & cache);


    
    /** Create the cointersection matrix (A^TA) required for the SVD.  This
        will call calcOverlap between every two behaviors in the dense
        behavior set.
    */
    void calcDenseCointersections(const BehaviorDomain & behs,
                                  const BehaviorCache & cache);

    float calcOverlapCached(BH bi,
                            BH bj,
                            const SvdColumnEntry & ei,
                            const SvdColumnEntry & ej,
                            const BehaviorDomain & behs) const;

    float calcOverlapCached(BH bi, BH bj,
                            const BehaviorCache & cache,
                            const BehaviorDomain & behs) const;

    int64_t memusage() const;

    /** Given a behavior, recalculate its singular vector. */
    distribution<float>
    calculateBehaviorVectorCached(BH beh, const BehaviorDomain & behs,
                                   const BehaviorCache & cache) const;

};

DECLARE_STRUCTURE_DESCRIPTION(BehaviorSvd);

} // namespace MLDB


#endif /* __behavior__behavior_svd_h__ */
