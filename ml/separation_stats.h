/* separation_stats.h                                              -*- C++ -*-
   Jeremy Barnes, 13 June 2011
   Copyright (c) 2011 mldb.ai inc.  All rights reserved.

   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

   Stats for classifier separation.
*/

#pragma once

#include "mldb/jml/utils/less.h"
#include "mldb/jml/math/xdiv.h"
#include "mldb/jml/utils/rng.h"
#include "mldb/ext/jsoncpp/json.h"
#include <cmath>
#include <boost/any.hpp>


namespace MLDB {


/*****************************************************************************/
/* BINARY STATS                                                              */
/*****************************************************************************/

/** Stats based just upon inclusion/exclusion. */

typedef std::array<std::array<double, 2>, 2> Array2D;

struct BinaryStats {
   
    BinaryStats()
        : counts{{{{0.0, 0.0}}, {{0.0, 0.0}}}},
        unweighted_counts{{{{0.0, 0.0}}, {{0.0, 0.0}}}},
          threshold(0)
    {
    }

    BinaryStats(const BinaryStats & other, float threshold,
                boost::any key = boost::any())
        : counts(other.counts), unweighted_counts(other.unweighted_counts),
          /*
        : counts{{{ other.counts[0][0], other.counts[0][1]},
                  { other.counts[1][0], other.counts[1][1]}}},
          unweighted_counts{{{ other.unweighted_counts[0][0], other.unweighted_counts[0][1]},
                             { other.unweighted_counts[1][0], other.unweighted_counts[1][1]}}},
          */
        threshold(threshold),
        key(std::move(key))
    {
    }

    void update(bool label, bool inSegment, double weight = 1.0)
    {
        counts[label][inSegment] += weight;
        counts[label][inSegment] += 1;
    }

    double includedPopulation(bool weighted=true) const
    {
        return truePositives(weighted) + falsePositives(weighted);
    }

    double excludedPopulation(bool weighted=true) const
    {
        return falseNegatives(weighted) + trueNegatives(weighted);
    }

    double accuracy() const
    {
        return ML::xdiv(truePositives() + trueNegatives(),
                        totalPositives() + totalNegatives());
    }

    double precision() const
    {
        return ML::xdiv(truePositives(), truePositives() + falsePositives());
    }

    double recall() const
    {
        return ML::xdiv(truePositives(), totalPositives());
    }

    double f() const
    {
        double p = precision(), r = recall();
        return 2.0 * ML::xdiv(p * r, p + r);
    }
    
    double specificity() const
    {
        return ML::xdiv(trueNegatives(), trueNegatives() + falsePositives());
    }

    // http://en.wikipedia.org/wiki/Matthews_correlation_coefficient
    double mcc() const
    {
    	double num = truePositives() * trueNegatives() -
    				falsePositives() * falseNegatives();
    	double den = std::sqrt( (truePositives()+falsePositives()) *
                                (truePositives()+falseNegatives()) *
                                (trueNegatives()+falsePositives()) *
                                (trueNegatives()+falseNegatives()));
    	return ML::xdiv(num, den);
    }

    const Array2D & getCounts(bool weighted) const {
        if(weighted) return counts;
        return unweighted_counts;
    }

    double truePositives(bool weighted=true) const { return getCounts(weighted)[true][true]; }
    double falsePositives(bool weighted=true) const { return getCounts(weighted)[false][true]; }
    double trueNegatives(bool weighted=true) const { return getCounts(weighted)[false][false]; }
    double falseNegatives(bool weighted=true) const { return getCounts(weighted)[true][false]; }

    double totalPositives(bool weighted=true) const
    {
        return counts[true][true] + counts[true][false];
    }

    double totalNegatives(bool weighted=true) const
    {
        return counts[false][true] + counts[false][false];
    }

    double totalPopulation(bool weighted=true) const
    {
        return totalPositives(weighted) + totalNegatives(weighted);
    }

    double truePositiveRate(bool weighted=true) const
    {
        return ML::xdiv(truePositives(weighted), totalPositives(weighted));
    }

    double falsePositiveRate(bool weighted=true) const
    {
        return ML::xdiv(falsePositives(weighted), totalNegatives(weighted));
    }

    double proportionOfPopulation(bool weighted=true) const
    {
        return ML::xdiv(truePositives(weighted) + falsePositives(weighted), totalPopulation(weighted));
    }

    double proportionOfPositives(bool weighted=true) const
    {
        return ML::xdiv(truePositives(weighted), totalPositives(weighted));
    }

    double proportionOfNegatives(bool weighted=true) const
    {
        return ML::xdiv(falsePositives(weighted), totalNegatives(weighted));
    }

    double gain() const
    {
        return ML::xdiv(proportionOfPositives(), proportionOfPopulation());
    }

    /** Calculates the area under the ROC curve between this point and
        another point. */
    double rocAreaSince(const BinaryStats & other) const;

    Array2D counts; // [label][output]
    Array2D unweighted_counts; // [label][output]
    double threshold;  // threshold at which stats are taken
    boost::any key;    // Key for this reading

    /** Add together the stats from another object to this one. */
    void add(const BinaryStats & other, double weight = 1.0);

    Json::Value toJson() const;
};


/*****************************************************************************/
/* SCORED STATS                                                              */
/*****************************************************************************/

/** Stats for a model that gives out a score. */

struct ScoredStats {

    ScoredStats();

    /** Calculate the stats for everything above the threshold. */
    BinaryStats atThreshold(float threshold) const;

    /** Calculate the stats for the given percentile. */
    BinaryStats atPercentile(float percent) const;

    /** Update with the given values. */
    void update(bool label, float score, double weight = 1.0,
                const boost::any & key = boost::any())
    {
        ScoredEntry entry;
        entry.label = label;
        entry.score = score;
        entry.weight = weight;
        entry.key = key;
        
        if (isSorted && !entries.empty() && entry < entries.back())
            isSorted = false;

        entries.push_back(entry);
    }

    struct ScoredEntry {
        boost::any key;  ///< What this applies to
        bool label;      ///< Label for the entry
        float score;     ///< Score for the entry
        float weight;

        bool operator < (const ScoredEntry & other) const
        {
            return ML::less_all(-score, -other.score, label, other.label);
        }
    };

    /// Stats broken down for a given threshold
    std::vector<BinaryStats> stats;

    /// All scored entries; sorted if isSorted is true
    std::vector<ScoredEntry> entries;

    /// Best F point
    BinaryStats bestF;

    /// Best MCC point
    BinaryStats bestMcc;

    // Best specificity point
    BinaryStats bestSpecificity; 

    /// Area under the ROC curve
    double auc;

    /// Is it sorted?
    bool isSorted;

    void sort();

    /** Add the other stats to this one.  sort() or calculate() must
        have been called on other.
    */
    void add(const ScoredStats & other);

    /** Calculate everything given the scored entries. */
    void calculate();

    /** Dump a ROC curve in a JS format our visualization can use. */
    void dumpRocCurveJs(std::ostream & stream) const;

    /** Save a ROC curve in the JS format for visualization. */
    void saveRocCurveJs(const std::string & filename) const;

    // Save and dump ROC curve in valid Json format. Similar to methods above
    // but not meant to be imported in node
    Json::Value getRocCurveJson() const;
    void saveRocCurveJson(const std::string & filename) const;

    Json::Value toJson() const;
};

} // namespace MLDB
