/* cluster.cc                                              -*- C++ -*-
   Jeremy Barnes, 6 November 2015
   Copyright (c) 2015 mldb.ai inc.  All rights reserved.

   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
*/


#include "cluster.h"
#include "mldb/plugins/jml/algebra/least_squares.h"
#include "factor_packing.h"
#include "raw_mapped_int_table.h"
#include "mapped_selector_table.h"
#include "mldb/arch/ansi.h"
#include "mldb/utils/ostream_set.h"
#include "mldb/utils/ostream_array.h"
#include <iostream>
#include <cassert>
#include <set>
#include <sstream>

using namespace std;

namespace MLDB {

using namespace ML;
using namespace ansi;

std::ostream & bold_red (std::ostream & stream) { return stream << bold << red; }
std::ostream & bold_bright_red (std::ostream & stream) { return stream << bold << bright_red; }

void Cluster::reserve(size_t size)
{
    points.reserve(size);
    residuals.reserve(size);
}

void Cluster::reset()
{
    points.clear();
    residuals.clear();
    residualStats.clear();
    indexStats.clear();
    valueStats.clear();
}

void Cluster::shrinkToFit()
{
    // Nothing to shrink to...
    if (points.empty())
        return;
    desiredResidualRange_ = maxResidualRange();
    desiredMinResidual = residualStats.minValue;
    desiredMaxResidual = residualStats.maxValue;

    for (auto [x,y]: points) {
        auto residual = predict(x) - y;
        if (!fitsInCluster(residual)) {
            cerr << "x = " << x << endl;
            cerr << "y = " << y << endl;
            cerr << "params = " << params << endl;
            cerr << "label = " << label << endl;
            cerr << "predict = " << predict(x) << endl;
            cerr << "residual = " << residual << endl;
            cerr << "desiredMinResidual = " << desiredMinResidual << endl;
            cerr << "desiredMaxResidual = " << desiredMaxResidual << endl;
            cerr << "desiredResidualRange = " << desiredResidualRange() << endl;
        }
        ExcAssert(fitsInCluster(residual));
    }
}

void Cluster::broaden()
{
    desiredResidualRange_ = (int32_t)MAX_LIMIT;
    desiredMinResidual = MAX_LIMIT;
    desiredMaxResidual = MIN_LIMIT;
}

float Cluster::bitsRequiredForResidual() const
{
    return 64.0f / get_factor_packing_solution(residualRange()).numFactors;
}

bool Cluster::fitsInCluster(int64_t residual) const
{
    // Don't add if it would make the residual range exceed 32 bits
    int64_t newMinResidual = std::min(residualStats.minValue, residual);
    int64_t newMaxResidual = std::max(residualStats.maxValue, residual);
    if (newMaxResidual - newMinResidual > std::numeric_limits<uint32_t>::max())
        return false;

    //cerr << "fitsInCluster residual " << residual << " expected " << desiredMinResidual << " to " << desiredMaxResidual << endl;
    if (desiredMinResidual > desiredMaxResidual) {
        //cerr << "  empty range, returning true" << endl;
        return true;
    }
    if (residual >= desiredMinResidual && residual <= desiredMaxResidual) {
        //cerr << "fits inside, returning true" << endl;
        return true;
    }
        
    //cerr << "returning false" << endl;
    return false;
}

bool Cluster::couldFitInCluster(int64_t residual) const
{
    if (residual >= residualStats.minValue && residual <= residualStats.maxValue)
        return true;
    if (fitsInCluster(residual))
        return true;
    auto newMinResidual = std::min(std::min(residual, residualStats.minValue), desiredMinResidual);
    auto newMaxResidual = std::max(std::max(residual, residualStats.maxValue), desiredMaxResidual);
    auto newRange = newMaxResidual - newMinResidual;
    return newRange <= desiredResidualRange();
}

#if 0
int64_t Cluster::bitsAddedToContainResidual(int64_t residual)
{
    if (residual >= residualStats.minValue && residual <= residualStats.maxValue)
        return 0;
    auto newMinResidual = std::min(residual, residualStats.minValue);
    auto newMaxResidual = std::max(residual, residualStats.maxValue);
    auto newRange = newMaxResidual - newMinResidual;
    auto oldBits = raw_mapped_indirect_bytes(points.size(), residualRange()) * 8;
    auto newBits = raw_mapped_indirect_bytes(points.size(), newRange) * 8;
    return newBits - oldBits;
}
#endif

void Cluster::addPoint(uint32_t x, uint32_t y)
{
    int64_t predicted = predict(x);
    int64_t residual = predicted - y;
    residualStats(residual);
    indexStats(x);
    valueStats(y);

    //cerr << "    adding point " << x << "," << y << " prediction " << predicted << " residual " << residual << endl;

    points.push_back(Point{x, y});
    residuals.push_back(residual);
}

void Cluster::addPoints(std::span<const Point> points)
{
    this->points.reserve(this->points.size() + points.size());
    this->residuals.reserve(this->residuals.size() + points.size());
    for (auto [x,y]: points) {
        addPoint(x,y);
    }
}

bool Cluster::quickFit()
{
    if (points.size() < 2) {
        std::fill(std::begin(params), std::end(params), 0);
        // intercept() is a float, and so can't represent all values; add an offset
        if (!points.empty()) {
            intercept() = points.empty() ? 0 : points[0].y;
            offset = predict(0) - points[0].y;
        }
        auto oldPoints = std::move(points);
        reset();
        for (auto [x,y]: oldPoints) {
            addPoint(x,y);
        }
        return true;
    }

    double n = points.size();

    // OLS fitting (closed form)
    double sumx = 0.0, sumy = 0.0;
    for (auto [x,y]: points) {
        sumx += x;
        sumy += y;
    }
    
    double meanx = sumx / n;
    double meany = sumy / n;

    double covxy = 0.0, varx = 0.0;
    auto sqr = [] (auto x) { return x * x; };
    for (auto [xval,yval]: points) {
        covxy += sqr(xval - meanx);
        varx += sqr(xval - meanx);
    }

    auto slope = covxy / varx;
    auto intercept = meany - slope * meanx;

    std::array<float, BASIS_DIMS> params = { (float)intercept, (float)slope };
    std::fill(params.begin() + 2, params.end(), 0);

    return commitParams(params);
}

bool Cluster::fit()
{
    //cerr << "fitting " << points.size() << " points" << endl;

    // Fitting one point is underconstrained, we assume zero slope which works
    // for guard values.
    if (points.size() <= BASIS_DIMS) {
        return quickFit();
    }

    size_t dims = Predictor::BASIS_DIMS;
    boost::multi_array<float, 2> A(boost::extents[points.size()][dims]);
    distribution<float> b(points.size());

    //A.fill({0}, [&] (size_t i) -> std::array<float, 2> { return { 1, points[i].x }; });
    //y.fill({0}, [&] (size_t i) { return points[i].y; });

    for (size_t i = 0;  i < points.size();  ++i) {
        auto [x,y] = points[i];
        auto basis = Predictor::basis(x);
        for (size_t j = 0;  j < basis.size();  ++j)
            A[i][j] = basis[j];
        b[i] = y;
    }

    auto x = least_squares(A, b);

    std::array<float, BASIS_DIMS> newParams;
    for (size_t i = 0;  i < BASIS_DIMS;  ++i)
        newParams[i] = x[i];

    return commitParams(newParams);
}

bool Cluster::commitParams(const std::array<float, BASIS_DIMS> & newParams)
{
    //cerr << "least squares x = " << x << endl;
    auto oldParams = params;

    int64_t oldMinResidual = MAX_LIMIT;
    int64_t oldMaxResidual = MIN_LIMIT;
    for (auto [x,y]: points) {
        int64_t residual = predict(x) - y;
        oldMinResidual = std::min(oldMinResidual, residual);
        oldMaxResidual = std::max(oldMaxResidual, residual);
    }

    if (oldMinResidual != residualStats.minValue || oldMaxResidual != residualStats.maxValue) {
        for (auto p: oldParams) {
            if (p == 0)
                continue;
            cerr << "residual mismatch" << endl;
            cerr << "label " << label << endl;
            cerr << "residualStats.minValue " << residualStats.minValue << " oldMinResidual " << oldMinResidual << endl;
            cerr << "residualStats.maxValue " << residualStats.maxValue << " oldMaxResidual " << oldMaxResidual << endl;
            cerr << "oldParams " << oldParams << endl;
            cerr << "newParams " << newParams << endl;
            cerr << "num points " << points.size() << endl;
            MLDB_THROW_LOGIC_ERROR("residuals don't match");
        }
    }

    params = newParams;

    uint64_t oldRange = residualRange();
    ExcAssert(int64_t(oldRange) == oldMaxResidual - oldMinResidual);

    int64_t newMinResidual = MAX_LIMIT;
    int64_t newMaxResidual = MIN_LIMIT;
    for (auto [x,y]: points) {
        int64_t residual = predict(x) - y;
        newMinResidual = std::min(newMinResidual, residual);
        newMaxResidual = std::max(newMaxResidual, residual);
    }

    uint64_t newRange = newMaxResidual - newMinResidual;

    //if (newRange >= std::numeric_limits<uint32_t>::max() || oldRange >= std::numeric_limits<uint32_t>::max()) {
    //    cerr << "oldRange = " << oldRange << " newRange = " << newRange << endl;
    //    cerr << "oldParams = " << oldParams << " newParams = " << params << endl;
    //}

    if (newRange >= oldRange) {

        // This can happen because least squares is biased to reduce the sum of
        // squares of the error, not their overall range.  In the future we may
        // adjust, but for now we just put it back to what it was before we
        // fit it.
        params = oldParams;
        return false;  // no change
#if 0
        auto dparams = params;
        for (size_t i = 0;  i < oldParams.size();  ++i) {
            dparams[i] -= oldParams[i];
        }

        cerr << "fit caused cluster range to expand" << endl;
        cerr << "  num points " << points.size() << endl;
        cerr << "  newRange " << newRange << endl;
        cerr << "  oldRange " << oldRange << endl;
        cerr << "  params " << params << endl;
        cerr << "  maxResidualRange " << maxResidualRange << endl;
        cerr << "  oldParams " << oldParams << endl;
        cerr << "  diff " << dparams << endl;
        cerr << "  old range " << oldMaxResidual - oldMinResidual << endl;
        //MLDB_THROW_LOGIC_ERROR("fit caused range to increase not decrease");
#endif
    }

    // Finally, reassign the points so our statistics are good
    auto oldPoints = std::move(points);
    reset();
    addPoints(oldPoints);
    return true;
}

std::vector<Cluster> Cluster::removeOutliers(double numDeviations, bool trace) const
{
    std::vector<Cluster> result;
    if (points.size() < 4 || residualStats.minValue == residualStats.maxValue)
        return result;

    int64_t totalResidual = 0;

    for (int64_t r: residuals) {
        totalResidual += r;
    }

    // TODO: single pass sum (x - mean)^2 = sum(x^2 - 2xmean + mean^2) = sum(x^2) - 2 mean sum(x) + mean^2

    double meanResidual = 1.0 * totalResidual / residuals.size();
    double stdResidual = 0.0;
    for (auto residual: residuals) {
        stdResidual += (residual - meanResidual) * (residual - meanResidual);
    }
    stdResidual = sqrt(stdResidual / residuals.size());

    //cerr << "residual: mean " << meanResidual << " std " << stdResidual << endl;

    int64_t minResidual = meanResidual - numDeviations * stdResidual;
    int64_t maxResidual = meanResidual + numDeviations * stdResidual; 

    ClusterSplitter splitter(*this);
    splitter.trace = trace;
    splitter.testSplit(ClusterSplitter::residualRangeSplitter(minResidual, maxResidual), "remOut rng", 0, 0);
    if (minResidual >= residualStats.minValue)
        splitter.testSplit(ClusterSplitter::residualSplitter(minResidual), "remOut bottom", 0, 0);
    if (maxResidual <= residualStats.maxValue)
        splitter.testSplit(ClusterSplitter::residualSplitter(maxResidual), "remOut top", 0, 0);

    if (splitter.bestScore > 0) {
        for (size_t i = 0;  i < splitter.bestClusters.size();  ++i) {
            if (splitter.bestClusters[i].points.size() > points.size() / 2) {
                splitter.bestClusters[i].label = label + " -ol";
                splitter.bestClusters[i].iter = iter;
            }
        }
        result = std::move(splitter.bestClusters);
    }
    return result;

#if 0



    return result;

    auto residualsSorted = residuals;
    std::sort(residualsSorted.begin(), residualsSorted.end());

    auto medianResidual = residualsSorted[residualsSorted.size() / 2];

    std::map<int64_t, size_t> residualCounts;
    for (auto residual: residualsSorted) {
        residualCounts[residual] += 1;
    }

    cerr << "residualCounts.size() = " << residualCounts.size() << endl;
    cerr << "medianResidual = " << medianResidual << endl;

    auto top = residualCounts.find(medianResidual);  // first and last give the range of the residual
    auto bottom = top;
    size_t numWithin = top->second;
    size_t numBelow = 0;
    size_t numAbove = 0;
    for (auto it = bottom;  it != residualCounts.begin();  /* no inc */) {
        --it;
        numBelow += it->second;
    }

    auto next  = [] (const auto & it) { auto result = it;  return ++result; };
    auto prior = [] (const auto & it) { auto result = it;  return --result; };

    for (auto it = next(top);  it != residualCounts.end();  ++it) {
        numAbove += it->second;
    }

    cerr << "numBelow = " << numBelow << " numAbove = " << numAbove << " numWithin = " << numWithin << endl;

    ExcAssert(numWithin + numAbove + numBelow == points.size());

    size_t maxPointsInRange = maxProportion * points.size();
    size_t bytesBefore = raw_mapped_indirect_bytes(points.size(), residualStats.range());

    // test a solution
    auto scoreSolution = [this, bytesBefore] (auto bottomIt, auto topIt, size_t numBelow, size_t numWithin, size_t numAbove) -> int64_t
    {
        auto residualRange = topIt->first - bottomIt->first;
        auto aboveResidualRange = residualStats.maxValue - topIt->first;
        auto belowResidualRange = bottomIt->first - residualStats.minValue;

        size_t bytesBelow = raw_mapped_indirect_bytes(numBelow, belowResidualRange);
        size_t bytesAbove = raw_mapped_indirect_bytes(numAbove, aboveResidualRange);
        size_t bytesWithin = raw_mapped_indirect_bytes(numWithin, residualRange);
        size_t totalBytes = bytesBelow + bytesWithin + bytesAbove + 2;

        int64_t score = bytesBefore - totalBytes; // the higher the better; it's the number of bytes saved

        return score;
    };

    // We have the points with this value of residual.  Expand our range until we have enough
    // points or we cause a big jump in residual range.
    while (numWithin < maxPointsInRange) {
        int64_t scoreBelow = MIN_LIMIT;
        int64_t scoreAbove = MIN_LIMIT;

        cerr << "testing residuals between " << bottom->second << " and " << top->second << endl;
        cerr << "  points: below " << numBelow << " within " << numWithin << " above " << numAbove << endl;

        if (bottom != residualCounts.begin()) {
            auto testBottom = prior(bottom);
            auto toTransfer = testBottom->second;
            scoreBelow = scoreSolution(testBottom, top, numBelow - toTransfer, numWithin + toTransfer, numAbove);
        }
        auto testTop = next(top);
        if (testTop != residualCounts.end()) {
            auto toTransfer = testTop->second;
            scoreAbove = scoreSolution(bottom, testTop, numBelow, numWithin + toTransfer, numAbove - toTransfer);
        }

        if (scoreBelow > scoreAbove) {
            --bottom;
            numWithin += bottom->second;
            numBelow -= bottom->second;
        }
        else {
            ++top;
            numWithin += top->second;
            numAbove -= top->second;
        }
    }

    // Four possible results:
    // 1.  Split into three (above, within, below)
    // 2.  Split into above+within and below
    // 3.  Split into above and within+below
    // 4.  Don't split because it's not worth it

    ClusterSplitter splitter(*this);
    splitter.testSplit(ClusterSplitter::residualRangeSplitter(bottom->second, top->second), "remOut rng", 0, 0);
    if (bottom != residualCounts.begin())
        splitter.testSplit(ClusterSplitter::residualSplitter(bottom->second), "remOut bottom", 0, 0);
    if (next(top) != residualCounts.end())
        splitter.testSplit(ClusterSplitter::residualSplitter(top->second), "remOut top", 0, 0);

    if (splitter.bestScore > 0) {
        result = std::move(splitter.bestClusters);
    }
    return result;
#endif
}

int64_t Cluster::scoreByMemoryUsage(const Cluster & oldCluster, const std::span<const Cluster> & newClusters)
{
    //cerr << "  " << newClusters.size() << " subclusters totalBytes " << totalBits/8 << " totalPoints " << totalPoints
    //     << " largestClusterSize " << largestClusterSize << " largestResidualRange " << largestResidualRange
    //     << " indexedBytes " << indexedBytes << " rawBytes " << rawBytes << " oldBytes " << oldCluster.storageBits() / 8
    //     << " score " << int64_t(oldCluster.storageBits() / 8 - bestBytes) << endl;

    return oldCluster.storageBits() / 8 - storageBits(newClusters) / 8;
}

std::array<Cluster, 2>
Cluster::splitOnIndex(size_t splitIndex) const
{
    if (splitIndex > size())
        throw std::out_of_range("Cluster::splitOnIndex");

    std::array<Cluster, 2> result;
    result[0].reserve(splitIndex);
    result[1].reserve(size() - splitIndex);

    for (size_t i = 0;  i < size();  ++i) {
        result[i >= splitIndex].addPoint(points[i]);
    }

    return result;
}

std::vector<std::pair<int64_t, std::array<Cluster, 2> > >
Cluster::bestSplitPoints(const ScoreFn & scorer, const std::vector<uint32_t> & candidatePoints, size_t numDesired) const
{
    std::multimap<int64_t, std::array<Cluster, 2> > result;
    if (numDesired == 0)
        return {};

    //cerr << "bestSplitPoints: candidatePoints.size() = " << candidatePoints.size() << endl;

    // Initialize score with what we'd score if we split this cluster into just itself
    auto worstPossibleScore = scorer(*this, { this, 1 });

#if 0
    std::vector<double> scores(points.size());
    for (auto [x,y]: points)
        scores.emplace_back(predict(x));

    struct Stats {
        uint64_t sumX = 0;
        uint64_t sumY = 0;
        size_t size = 0;

        void addPoint(uint32_t x, uint32_t y);
        void addPoints(std::span<const Point> & points);
        void removePoint(uint32_t x, uint32_t y);
        void removePoints(std::span<const Point> & points);

        std::vector<double> points;

        std::array<float, BASIS_DIMS> fit() const;
    };

    // These clusters are accumulators
    Cluster before = *this, after;

    Stats beforeStats, afterStats;
    beforeStats.addPoints(before.points);

    for (auto it = candidatePoints.rbegin();  it != candidatePoints.rend();  ++it) {
        auto splitIndex = *it;
        // At each point, transfer from the end of before to the beginning of after

    }
#endif

    auto addResultTo = [] (int64_t score, std::array<Cluster, 2> & clusters, size_t maxSize,
                           std::multimap<int64_t, std::array<Cluster, 2> > & result)
    {
        if (result.size() < maxSize || score > result.begin()->first) {
            if (result.size() >= maxSize)
                result.erase(result.begin());
            result.emplace(score, std::move(clusters));
        }
    };

    // 2.  Attempt the split at each of the candidate points
    for (size_t splitPoint: candidatePoints) {
        if (splitPoint == 0 || splitPoint == size())
            continue;

        auto clusters = splitOnIndex(splitPoint);

        for (Cluster & c: clusters) {
            ExcAssert(!c.empty());
            c.quickFit();  // fast fit until we see how good it really is
        }

        auto score = scorer(*this, clusters);
        if (score <= worstPossibleScore)
            continue;

        addResultTo(score, clusters, numDesired * 2, result);
    }

    // 3.  Reduce down the set, but scoring them first with the real (more expensive) fit
    if (result.size() > numDesired) {
        std::multimap<int64_t, std::array<Cluster, 2> > reduced;
        for (auto [oldScore, clusters]: result) {
            for (auto & c: clusters)
                c.fit();
            auto newScore = scorer(*this, clusters);
            addResultTo(newScore, clusters, numDesired, reduced);        
        }

        result = std::move(reduced);
    }
    
    return std::vector<std::pair<int64_t, std::array<Cluster, 2> > >(result.begin(), result.end());
}

struct Cluster::PiecewiseState {
    std::map<std::pair<uint32_t, uint32_t>, std::vector<Cluster>> done;
};

std::vector<Cluster> Cluster::fitPiecewise(size_t desiredResidualRange, ScoreFn scorer, bool trace, PiecewiseState * state,
                                           size_t maxRecursion, size_t recursion) const
{
    std::vector<Cluster> result;

    if (residualRange() <= desiredResidualRange)
        return result;

    if (recursion > maxRecursion)
        return result;

    ExcAssert(points.size() == residualStats.count);
    ExcAssert(points.size() == residuals.size());
    size_t n = points.size();

    if (n < 3  // Not enough points
        || residualRange() < desiredResidualRange  // Fit is already good enough
        || storageBits() < sizeof(Predictor) * 8 * 3) {  // Can't save memory no matter what
        return result;
    }

    PiecewiseState newState;

    if (!state) {
        state = &newState;
    }

    std::pair<uint32_t, uint32_t> key(points.front().x, points.back().x);
    if (state->done.count(key)) {
        result = state->done[key];
        return result;
    }

    //if (state->recursion > 4)
    //    return result;

    //trace = state->recursion < 2;

    // Find the best way to fit the first part.  It's assumed that if we fit the first part well,
    // the second part will take care of itself.
    std::set<uint32_t> highPoints, lowPoints;

    // We don't start at the start, as splitting off points at the very beginning has a quadratic
    // runtime.  For the rest, we attempt to avoid adding lots of points when we flip back and
    // forth between high and low values (TODO)
    for (size_t i = 1;  i < points.size() - 1;  ++i) {
        auto r = residuals[i];
        if (r == residualStats.minValue || r == residualStats.maxValue) {
            // If it's the same as the point before and the point after, it's a run
            // and we don't test all the points (we just test the endpoints of the run).
            if (i > 0 && i < points.size() - 1 && r == residuals[i - 1] && r == residuals[i + 1])
                continue;

            // We look at a split before the given point; we may look after on the next phase
            if (i > 0)
                (r == residualStats.minValue ? lowPoints : highPoints).insert(i);
        }
    }

#if 1
    // Reduce to a reasonable number by looking for longer gaps
    auto reducePoints = [&] (std::set<uint32_t> & splits, size_t desiredNumPoints)
    {
        if (splits.size() <= desiredNumPoints)
            return;
        std::vector<std::pair<uint32_t, uint32_t> > spaces;
        uint32_t lastX = 0;
        for (auto i: splits) {
            auto [x,y] = points[i];
            auto space = x - lastX;
            spaces.emplace_back(space, i);
        }
        std::sort(spaces.rbegin(), spaces.rend());

        std::set<uint32_t> result;
        for (auto [space, i]: spaces) {
            result.insert(i);
            result.insert(i + 1);
            if (result.size() >= desiredNumPoints)
                break;
        }
        splits = std::move(result);
    };

    reducePoints(highPoints, 20);
    reducePoints(lowPoints, 20);
#endif

    auto candidatePoints = highPoints;  candidatePoints.insert(lowPoints.begin(), lowPoints.end());

    //for (auto pt: candidatePoints) {
    //    cerr << pt << ":" << residuals[pt] << " ";
    //}
    //cerr << endl;

    // Shrink the number of candidates down to something reasonable
    auto candidateSplits = bestSplitPoints(scorer, std::vector<uint32_t>(candidatePoints.begin(), candidatePoints.end()), 4 /* max desired */);

    if (candidateSplits.empty())
        return result;

    if (trace) {
        cerr << "fitPiecewise of " << points.size() << " points from " << indexStats.minValue << " to " << indexStats.maxValue
            << " with " << candidatePoints.size() << " candidate points (" << candidateSplits.size() << " post-filtering with"
            << " best score " << candidateSplits[0].first
            << " and recursion " << recursion << " of " << maxRecursion << " and cache size " << state->done.size() << endl;
    }

    std::string indent;
    if (trace) {
        indent = std::string(recursion * 2, ' ');
        cerr << indent << "candidate points: " << candidatePoints << endl;
    }

    // Initialize score with what we'd score if we split this cluster into just itself
    auto bestScore = scorer(*this, { this, 1 });

    auto testSplit = [&] (int64_t preScore, Cluster & cBefore, Cluster & cAfter)
    {
        std::vector<Cluster> splitResult;

        auto processSegment = [&] (Cluster & c)
        {
            c.shrinkToFit();
            auto subClusters = c.fitPiecewise(desiredResidualRange, scorer, trace, state, maxRecursion, recursion + 1);
            if (subClusters.empty()) {
                splitResult.emplace_back(std::move(c));
            }
            else {
                splitResult.insert(splitResult.end(),
                                   std::make_move_iterator(subClusters.begin()),
                                   std::make_move_iterator(subClusters.end()));
            }
        };

        processSegment(cBefore);
        processSegment(cAfter);

        auto score = scorer(*this, splitResult);

        if (trace) {
            cerr << indent << " split " << cBefore.size() << " pre-score " << preScore << ": " << splitResult.size() << " segments; score " << score
                 << " vs best " << bestScore << endl;
        }

        if (score > bestScore) {
            bestScore = score;
            result = std::move(splitResult);
        }
    };

    // 2.  Attempt the split at each of the candidate points
    for (auto & [score, clusters] : candidateSplits) {
        testSplit(score, clusters[0], clusters[1]);
    }

    // 3.  Post-process the best solution
    for (size_t i = 0;  i < result.size();  ++i) {
        Cluster & c = result[i];
        c.parent = this->label;
        c.iter = this->iter;
        c.label = "piecewise " + std::to_string(c.indexStats.minValue) + "-" + std::to_string(c.indexStats.maxValue);
    }

    state->done.emplace(key, result);

    if (recursion == 0 && trace)
        cerr << "cache size = " << state->done.size() << endl;

    return result;
}

std::vector<Cluster> Cluster::optimize(ScoreFn scorer, bool trace, PiecewiseState * state, size_t maxRecursion, size_t recursion) const
{
    // We attempt to gradually reduce the residual range until we find that
    // there is no more benefit to doing it more.
    std::vector<Cluster> result;

    if (recursion > maxRecursion)
        return result;

    PiecewiseState newState;
    if (state == nullptr) {
        state = &newState;
    }

    if (trace)
        cerr << "optimize with recursion " << recursion << " of " << maxRecursion << " and cache size " << state->done.size() << endl;

    auto currentRange = residualRange();
    std::vector<Cluster> currentBest = fitPiecewise(currentRange / 2, scorer, trace, state, maxRecursion, recursion + 1);
    if (currentBest.empty())
        return result;

    for (auto & c: currentBest) {
        std::vector<Cluster> split = c.optimize(scorer, trace, state, maxRecursion, recursion + 1);
        if (split.empty()) {
            result.emplace_back(std::move(c));
        }
        else {
            result.insert(result.end(), std::make_move_iterator(split.begin()), std::make_move_iterator(split.end()));
        }
    }

    auto oldScore = scorer(*this, { this, 1});
    auto newScore = scorer(*this, result);
    if (trace)
        cerr << "split into " << result.size() << " oldScore = " << oldScore << " newScore = " << newScore << endl;

    if (oldScore >= newScore) {
        result.clear();
    }

    if (state == &newState && trace)
        cerr << "cache size = " << state->done.size() << endl;

    return result;
}

// Maximum range we can have without needing more storage (ie, for free)
uint64_t Cluster::maxResidualRange() const
{
    if (residualStats.count == 0)
        return (uint32_t)MAX_LIMIT;
    return get_factor_packing_solution(residualRange()).factor - 1;
}  

int Cluster::numBits() const
{
    return bits(residualRange());
}

uint64_t Cluster::storageBits() const
{
    return storageBits({this, 1});
}

double Cluster::storageBitsPerPoint() const
{
    return 1.0 * storageBits() / points.size(); 
}

uint64_t Cluster::storageBits(const std::span<const Cluster> & clusters)
{
    size_t totalBits = 0;
    size_t totalPoints = 0;
    size_t largestClusterSize = 0;
    uint64_t largestResidualRange = 0;

    if (clusters.size() == 1) {
        return 8 * (raw_mapped_indirect_bytes(clusters[0].size(), clusters[0].residualRange())
                    + raw_mapped_indirect_bytes(clusters[0].size(), 0)
                    + sizeof(Predictor)
                    + sizeof(RawMappedIntTable));
    }

    // Look to see if cluster's points are trivially sorted (eg via a piecewise split)
    bool triviallySorted = true;
    size_t lastClusterEndsAt = 0;

    for (auto & c: clusters) {
        totalBits += c.storageBits();
        totalPoints += c.size();
        largestClusterSize = std::max(largestClusterSize, c.size());
        largestResidualRange = std::max(largestResidualRange, c.residualRange());
        if (!c.empty() && triviallySorted && c.indexStats.minValue >= lastClusterEndsAt) {
            lastClusterEndsAt = c.indexStats.maxValue;
        }
    }

    // Otherwise, we need to see if they are non-intersecting.
    bool nonIntersecting = true;
    if (!triviallySorted) {
        // TODO: can do better than this (it's quadratic in the number of clusters)
        for (auto & c: clusters) {
            if (!nonIntersecting)
                break;
            if (c.empty())
                continue;
            for (auto & c2: clusters) {
                if (&c == &c2)
                    continue;
                if (c2.empty())
                    continue;
                if (c.indexStats.maxValue < c2.indexStats.minValue
                    || c.indexStats.minValue > c2.indexStats.maxValue)
                    continue;  // non-overlapping
                nonIntersecting = false;
                break;
            }
        }
    }

    size_t selectorTableBytes = 0;

    if (triviallySorted || nonIntersecting) {
        // Selector tables will have one run per cluster in the value
        IntTableStats<uint32_t> rangeStats, valueStats;
        rangeStats.size = clusters.size() + 1;
        rangeStats.maxValue = totalPoints - 1;
        rangeStats.numRuns = clusters.size() + 1;

        valueStats.minValue = 0;
        valueStats.maxValue = clusters.size() - 1;
        valueStats.numRuns = clusters.size();

        selectorTableBytes = raw_mapped_indirect_bytes(rangeStats) + raw_mapped_indirect_bytes(valueStats);
    }
    else {
#if 0
        using PointIterator = std::vector<Point>::const_iterator;

        // Look for runs so we can properly size things.
        size_t numRuns = 0;
        size_t maxRunLength = 0;
        std::vector<PointIterator> current, end;
        current.reserve(clusters.size();
        end.reserve(clusters.size());

        for (auto & c: clusters) {
            current.push_back(c.points.begin());
            end.push_back(c.points.end());
        }
#else
        // TODO: we can do this in linear time, see above...
        std::vector<std::pair<uint32_t, uint8_t> > x_s;
        for (size_t i = 0;  i < clusters.size();  ++i) {
            const Cluster & c = clusters[i];
            for (auto [x,y]: c.points) {
                x_s.emplace_back(x, i);
            }
        }

        if (!std::is_sorted(x_s.begin(), x_s.end())) {
            std::sort(x_s.begin(), x_s.end());
        }

        std::vector<uint8_t> selectors(totalPoints);
        for (size_t i = 0;  i < x_s.size();  ++i) {
            selectors[i] = x_s[i].second;
        }

        selectorTableBytes = mapped_selector_table_bytes_required(selectors);
#endif
    }
    // We either store a) indexed selectors with a residual table for each, or b) non-indexed selectors with
    // a global residual table.  Score based upon the most efficient storage method.

    // By splitting, we also need to store selectors... 
    uint64_t indexedBytes
        = selectorTableBytes
        + sizeof(MappedSelectorTable)
        + totalBits / 8;

    uint64_t rawBytes
        = raw_mapped_indirect_bytes(totalPoints, largestResidualRange)
        + raw_mapped_indirect_bytes(totalPoints, clusters.size() - 1)
        + sizeof(Predictor)
        + 2 * sizeof(RawMappedIntTable);

    uint64_t bestBytes = std::min(indexedBytes, rawBytes);

    return bestBytes * 8;
}

void Cluster::commitSplit(std::vector<Cluster> split, std::vector<Cluster> & clusters)
{
    if (split.empty())
        return;
    
    // This one gets replaced with the first split
    *this = std::move(split[0]);

    // Others get added to the list of clusters
    clusters.insert(clusters.end(),
                    std::make_move_iterator(split.begin() + 1),
                    std::make_move_iterator(split.end()));
}

std::ostream & Cluster::printHeaders(std::ostream & stream, const std::string & indent)
{
    stream << indent << reversed << "==========|==== mem ====|========== x ==========|======= y =======|========= residuals =========|====== params ======|= cluster ==============" << ansi::reset << endl;
    stream << indent << reversed << "i   points|  bits sizekB|     min      max   dns|     min      max|   min-max   (range)  desired| x^0 x^1 x^2 x^3    |iter label              " << ansi::reset << endl;
    return stream;
}

std::string Cluster::print(size_t index) const
{
    char buf[256];

    std::ostringstream paramsStream;
    paramsStream << params;

    snprintf(buf, 256, "%03zd%7zd|%7.2f%7.2f|%8zd %8zd %5.3f|%8d %8d|%7lld:%-7lld%6lld%6d|%-20s|%02d %-20s",
                index, points.size(), storageBitsPerPoint(), storageBits() / 8192.0,
                indexStats.minValue, indexStats.maxValue, density(),
                valueStats.minValue, valueStats.maxValue,
                residualStats.minValue, residualStats.maxValue, residualRange(), desiredResidualRange(),
                std::string(paramsStream.str(), 2, 20).c_str(),
                iter, label.c_str());

    return buf;
}

void Cluster::printList(std::ostream & stream, const std::span<const Cluster> & clusters)
{
    size_t totalStorageBits = 0;
    size_t totalPoints = 0;
    for (auto & c: clusters) {
        totalStorageBits += c.storageBits();
        totalPoints += c.size();
    }
    double bitsPerEntry = 1.0 * totalStorageBits / totalPoints;

    stream << "Storage: " << totalStorageBits / 8192.0 << "kB clusters" << endl;
    stream << "   bits: " << bitsPerEntry << " bits/entry clusters" << endl;

    Cluster::printHeaders(stream);

    for (size_t i = 0;  i < clusters.size();  ++i) {
        if (clusters[i].points.empty())
            continue;
        const Cluster & c = clusters[i];
        static const std::vector<float> colorSplits = { 0, 0.2, 0.5, 2.0, 3.0, 4.0, 5.0, INFINITY };
        static const std::vector<std::ostream & (*) (std::ostream &)> colors = {
            bright_green, green, ansi::reset, red, bold_red, bright_red, bold_bright_red, bold_bright_red };
        ExcAssert(colorSplits.size() == colors.size());
        size_t n = std::lower_bound(begin(colorSplits), end(colorSplits), c.storageBitsPerPoint() / bitsPerEntry)
           - begin(colorSplits);
        ExcAssert(n < colors.size());
        stream << colors[n] << c.print(i) << ansi::reset << endl;
    }
}

void Cluster::printSplit(std::ostream & stream, const Cluster & original, const std::span<const Cluster> & split)
{
    size_t memoryBefore = original.storageBits();
    size_t memoryAfter = 0;
    for (auto & c: split) {
        memoryAfter += c.storageBits();
    }
    size_t n = original.points.size();

    stream << bold << underline << "cluster " + original.label + " with " << n << " points split into " << split.size()
            << ansi::reset << bright_white << " memBefore " << memoryBefore / 8192.0 << " memAfter " << memoryAfter / 8291.0
            << " bits before " << 1.0 * memoryBefore / n << " bits after " << 1.0 * memoryAfter / n << ansi::reset << endl;
    Cluster::printHeaders(stream);
    stream << bright_white << original.print(999) << ansi::reset << endl;
    for (size_t j = 0;  j < split.size();  ++j) {
        stream << yellow << split[j].print(j) << ansi::reset << endl;
    }
}

ClusterSplitter::ClusterSplitter(const Cluster & parent, ScoreFn scoreFn)
    : parent(&parent), scoreFn(std::move(scoreFn))
{
}

std::pair<std::vector<Cluster>, int64_t> ClusterSplitter::split(SplitFn splitFn, const char * splitType, int param, int iteration) const
{
    std::vector<Cluster> newClusters;
    Cluster toAdd = *parent;
    toAdd.iter = iteration;
    toAdd.label = string(splitType) + "(" + std::to_string(param) + ")";
    toAdd.parent = parent->parent + " " + parent->label;
    toAdd.reset();

    for (auto [x,y]: parent->points) {
        int64_t residual = parent->predict(x) - y;
        size_t clusterNum = splitFn(x, y, residual);
        while (clusterNum >= newClusters.size()) {
            newClusters.push_back(toAdd);
            newClusters.back().label += "#" + std::to_string(newClusters.size() - 1);
        }
        newClusters[clusterNum].addPoint(x, y);
    }

    for (Cluster & c: newClusters) {
        c.fit();
        c.shrinkToFit();
    }

    auto score = scoreFn(*parent, newClusters);

    if (trace) {
        cerr << "test of " << splitType << "(" << param << "): score " << score << endl;
        Cluster::printHeaders(cerr);
        cerr << bright_white << parent->print(-1) << reset << endl;
        for (size_t i = 0;  i < newClusters.size();  ++i) {
            cerr << newClusters[i].print(i) << endl;
        }
    }

    return { std::move(newClusters), score };
}

int64_t ClusterSplitter::testSplit(SplitFn splitFn, const char * splitType, int param, int iteration)
{
    auto [clusters, score] = split(splitFn, splitType, param, iteration);
    if (score > bestScore) {
        bestScore = score;
        bestClusters = std::move(clusters);
        bestSplitType = splitType;
        bestParam = param;
    }

    return score;
}

void ClusterSplitter::commit(Cluster & parent, std::vector<Cluster> & newSplitClusters)
{
    if (bestClusters.empty())
        return;
    cerr << "bestScore is " << bestScore << " at " << bestSplitType << "(" << bestParam << ")" << endl;
    parent = std::move(bestClusters[0]);
    newSplitClusters.insert(newSplitClusters.end(),
                            std::make_move_iterator(bestClusters.begin() + 1),
                            std::make_move_iterator(bestClusters.end()));
    bestClusters.clear();
    bestScore = MIN_LIMIT;
}

ClusterSplitter::SplitFn ClusterSplitter::xSplitter(uint32_t splitX)
{
    return [splitX] (uint64_t x, uint64_t, int64_t) { return unsigned(x >= splitX); };
}

ClusterSplitter::SplitFn ClusterSplitter::ySplitter(uint32_t splitY)
{
    return [splitY] (uint64_t, uint64_t y, int64_t) { return unsigned(y >= splitY); };
}

ClusterSplitter::SplitFn ClusterSplitter::residualSplitter(int64_t minResidual)
{
    return [minResidual] (uint32_t, uint32_t, int64_t residual)
    {
        return residual >= minResidual;
    };
}

ClusterSplitter::SplitFn ClusterSplitter::residualRangeSplitter(int64_t minResidual, int64_t maxResidual)
{
    return [minResidual, maxResidual] (uint32_t, uint32_t, int64_t residual)
    {
        if (residual < minResidual)
            return 0;
        else if (residual > maxResidual)
            return 2;
        else return 1;
    };
}

} // namespace MLDB
