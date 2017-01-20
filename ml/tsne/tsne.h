// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* tsne.h                                                          -*- C++ -*-
   Jeremy Barnes, 16 January 2010
   Copyright (c) 2010 Jeremy Barnes.  All rights reserved.

   Implementation of the TSNE dimensionality reduction algorithm, particularly
   useful for visualization of data.

   See http://ict.ewi.tudelft.nl/~lvandermaaten/t-SNE.html

   L.J.P. van der Maaten and G.E. Hinton.
   Visualizing High-Dimensional Data Using t-SNE.
   Journal of Machine Learning Research 9(Nov):2579-2605, 2008.
*/

#ifndef __jml__tsne__tsne_h__
#define __jml__tsne__tsne_h__

#include "mldb/jml/stats/distribution.h"
#include <boost/multi_array.hpp>
#include "vantage_point_tree.h"
#include "quadtree.h"

namespace ML {

template<typename Item> struct VantagePointTreeT;
struct Quadtree;

/** Calculate the probability distribution that gives the given
    perplexity.  The input in D is the *square* of the distances
    to the points.
*/
std::pair<double, distribution<float> >
perplexity_and_prob(const distribution<float> & D, double beta = 1.0,
                    int i = -1);

std::pair<double, distribution<double> >
perplexity_and_prob(const distribution<double> & D, double beta = 1.0,
                    int i = -1);

/** Given a matrix that gives the a number of points in a vector space of
    dimension d (ie, a number of points with coordinates of d dimensions),
    convert to a matrix that gives the square of the distance between
    each of the points.

                     2
    D   = ||X  - X || 
     ij      i    j

    params:
    X    a (n x d) matrix, where n is the number of points and d is the
         number of coordinates that each point has
    D    a (n x n) matrix that will be filled in with the distance between
         any of the two points.  Note that by definition the diagonal is
         zero and the matrix is symmetric; as a result only the lower
         diagonal needs to be filled in.
    fill_upper if set, the lower diagonal will be copied into the upper
               diagonal so that the entire matrix is filled in.
*/
void
vectors_to_distances(const boost::multi_array<float, 2> & X,
                     boost::multi_array<float, 2> & D,
                     bool fill_upper = true);

void
vectors_to_distances(const boost::multi_array<double, 2> & X,
                     boost::multi_array<double, 2> & D,
                     bool fill_upper = true);

inline boost::multi_array<float, 2>
vectors_to_distances(boost::multi_array<float, 2> & X,
                     bool fill_upper = true)
{
    int n = X.shape()[0];
    boost::multi_array<float, 2> result(boost::extents[n][n]);
    vectors_to_distances(X, result, fill_upper);
    return result;
}

inline boost::multi_array<double, 2>
vectors_to_distances(boost::multi_array<double, 2> & X,
                     bool fill_upper = true)
{
    int n = X.shape()[0];
    boost::multi_array<double, 2> result(boost::extents[n][n]);
    vectors_to_distances(X, result, fill_upper);
    return result;
}

/** Calculate the beta for a single point.
    
    \param Di     The i-th row of the D matrix, for which we want to calculate
                  the probabilities.
    \param i      Which row number it is' -1 means none

    \returns      The i-th row of the P matrix, which has the distances in D
                  converted to probabilities with the given perplexity, as well
                  as the calculated perplexity value.
 */
std::pair<distribution<float>, double>
binary_search_perplexity(const distribution<float> & Di,
                         double required_perplexity,
                         int i = -1,
                         double tolerance = 1e-5);

boost::multi_array<float, 2>
distances_to_probabilities(boost::multi_array<float, 2> & D,
                           double tolerance = 1e-5,
                           double perplexity = 30.0);

/** Perform a principal component analysis.  This routine will reduce a
    (n x d) matrix to a (n x e) matrix, where e < d (and is possibly far less).
    The num_dims parameter gives the preferred value of e; it is possible that
    the routine will return a smaller value of e than this (where the rank of
    X is lower than the requested e value).
*/
boost::multi_array<float, 2>
pca(boost::multi_array<float, 2> & coords, int num_dims = 50);

struct TSNE_Params {
    
    TSNE_Params()
        : numNeighbours(100),
          perplexity(20),
          tolerance(1e-5),
          randomSeed(0),
          min_iter(200),
          max_iter(1000),
          initial_momentum(0.5),
          final_momentum(0.8),
          eta(500),
          min_gain(0.01),
          min_prob(1e-12),
          min_distance_ratio(0.6),
          max_coord_change(0.0005)
    {
    }

    int numNeighbours;
    double perplexity;
    double tolerance;

    int randomSeed;

    int min_iter;
    int max_iter;
    double initial_momentum;
    double final_momentum;
    double eta;
    double min_gain;
    double min_prob;

    double min_distance_ratio;  // 0 means never approximate; 1 means approximate everything
    double max_coord_change;    // stop once no coordinate has changed its relative pos by this
};

// Function that will be used as a callback to provide progress to a calling
// process.  Arguments are:
// - int: iteration number
// - float: cost when last measured
// - const char *: phase name (of this iteration)
// - TSNE_Params &: parameters (may be modified)
// The return should be true to keep going, or false to stop (the most recent
// Y will then be returned).
typedef std::function<bool (int, float, const char *)>
TSNE_Callback;

boost::multi_array<float, 2>
tsne(const boost::multi_array<float, 2> & probs,
     int num_dims = 2,
     const TSNE_Params & params = TSNE_Params(),
     const TSNE_Callback & callback = TSNE_Callback());


struct TsneSparseProbs {
    std::vector<int> indexes;
    distribution<float> probs;
};

/** Sparse and approximate Barnes-Hut-SNE version of tSNE.
    Input is a sparse distribution of probabilities per example.
 */
boost::multi_array<float, 2>
tsneApproxFromSparse(const std::vector<TsneSparseProbs> & neighbours,
                     int num_dims,
                     const TSNE_Params & params = TSNE_Params(),
                     const TSNE_Callback & callback = TSNE_Callback(),
                     std::unique_ptr<Quadtree> * qtreeOut = nullptr);

boost::multi_array<float, 2>
tsneApproxFromDense(const boost::multi_array<float, 2> & probs,
                    int num_dims,
                    const TSNE_Params & params = TSNE_Params(),
                    const TSNE_Callback & callback = TSNE_Callback());

boost::multi_array<float, 2>
tsneApproxFromCoords(const boost::multi_array<float, 2> & coords,
                     int num_dims,
                     const TSNE_Params & params = TSNE_Params(),
                     const TSNE_Callback & callback = TSNE_Callback(),
                     std::unique_ptr<VantagePointTreeT<int>> * treeOut = nullptr,
                     std::unique_ptr<Quadtree> * qtreeOut = nullptr);

/** Structure used to implement a distance function between coordinates
    and two points defined by coordinates in an euclidian space.  It uses
    a factorization to speed up the calculation, which is cached in the
    object.
*/
struct PythagDistFromCoords {

    /** Construct the object.  This accepts a nx by nd matrix, and
        for each x, calculates the square of the two-norm of the
        vector itself.
    */
    PythagDistFromCoords(const boost::multi_array<float, 2> & coords);

    const boost::multi_array<float, 2> & coords;
    distribution<float> sum_dist;
    int nx;
    int nd;

    /** Distance between points.

        Given two points x and y, whose coordinates are coords[x] and
        coords[y], this will calculate the euclidian distance
        ||x - y|| = sqrt(sum_i (x[i] - y[i])^2)
                  = sqrt(sum_i (x[i]^2 + y[i]^2 - 2 x[i]y[i]) )
                  = sqrt(sum_i x[i]^2 + sum_i y[i]^2 - 2 sum x[i]y[i])
                  = sqrt(||x||^2 + ||y||^2 - 2 x . y)

        Must satisfy the triangle inequality, so the sqrt is important.  We
        also take pains to ensure that dist(x,y) === dist(y,x) *exactly*,
        and that dist(x,x) == 0.
    */
    float operator () (int x1, int x2) const;
};

/** Given a function that can calculate a distance between any two of nx
    points, a number of nearest neighbours and a perplexity score,
    calculate a sparse set of neighbour probabilities with the given
    perplexity for each of the elements.  This assumes that the points
    are on a gaussian.

    The points are first inserted into a vantage point tree to allow
    efficient nearest neighbour lookup.

    After that, for each point, the parameters of the gaussian that induce
    the given perplexity are learnt, and then the probability distribution
    is generated over the closest neighbours that uses the gaussian
    learnt to induce probabilities.

    Input:
    - dist: a function that can be applied to any two points, and will return
      the distance between them.  It is exremely important that the following
      characteristics hold:
      1.  dist(x,x) == 0 to full numeric precision
      2.  dist(y,x) == dist(x,y) to full numeric precision
      3.  dist(x,z) <= dist(x,y) + dist(y,z) to full precision, ie the triangle
          inequality holds.
    - nx: the number of points.  Numbers from [0,nx) will be passed to the
      distance function.
    - numNeighbours: number of neighbours to return for each of the
      examples.  This would typically be set at 3 * the perplexity to make
      sure that there are sufficiently distant examples for the chosen
      perplexity.
    - perplexity: value of perplexity to calculate.  The output
      distribution for each of the nx examples will have this perplexity.
    - tolerance: the tolerance value for the perplexity calculation.  A binary
      search algorithm is used; this affects the convergance characteristics.
    - treeOut: if this is non-null, the vantage point tree constructed will
      be put into the given unique_ptr for later re-use.


    Output:
    - A vector of nx entries, each of which contains numNeighbours pairs
      of (probability, exampleNum) where the probability distribution for
      a given example has the given perplexity.  The probabilities for
      each example will sum to one.
    - If treeOut is not null, then the unique_ptr it points to will be
      initialized with the vantage tree used to perform the calculations.
*/

std::vector<TsneSparseProbs>
sparseProbsFromDist(const std::function<float (int, int)> & dist,
                    int nx,
                    int numNeighbours,
                    double perplexity,
                    double tolerance = 1e-5,
                    std::unique_ptr<VantagePointTreeT<int> > * treeOut = nullptr);

/** Calculate a new set of sparse probabilities for an example, re-applying
    what was learned from a previous sparseProbsFromCoords implementation.

    Input:
    - coords, numNeighbours, perplexity, tolerance: as above
    - newExampleCoords: the coordinates of the new example to calculate
      probabilities for
    - tree: the vantage point tree output from sparseProbsFromCoords()
    - toRemove: if this is != -1, then this example is in the tree and
      needs to be removed (there will be a zero distance entry in each
      which must be got rid of).  If it is a new example that wasn't in
      those presented to sparseProbsFromCoords(), then it should be
      left at -1.  If it's not -1, then there is a prerequisite that
      dist(toRemove) == 0; in other words it must be the closest point.

    Output:
    - as in sparseProbsFromCoords()
*/
TsneSparseProbs
sparseProbsFromCoords(const std::function<float (int)> & dist,
                      const VantagePointTreeT<int> & tree,
                      int numNeighbours,
                      double perplexity,
                      double tolerance = 1e-5,
                      int toRemove = -1);

/** Re-run t-SNE over the given high dimensional probability vector for a
    single example, figuring out where that example should be embedded in
    a fixed containing space from the main tsne computation.

    The calculation is much simpler since we only have one point that can
    move at a time, rather than the whole lot of them.
*/
distribution<float>
retsne(const distribution<float> & probs,
       const boost::multi_array<float, 2> & prevOutput,
       const TSNE_Params & params = TSNE_Params());


distribution<float>
retsneApproxFromSparse(const TsneSparseProbs & neighbours,
                       const boost::multi_array<float, 2> & prevOutput,
                       const Quadtree & qtree,
                       const TSNE_Params & params);


distribution<float>
retsneApproxFromCoords(const distribution<float> & coords,
                       const boost::multi_array<float, 2> & coreCoords,
                       const boost::multi_array<float, 2> & prevOutput,
                       const Quadtree & qtree,
                       const VantagePointTreeT<int> & vpTree,
                       const TSNE_Params & params);


} // namespace ML

#endif /* __jml__tsne__tsne_h__ */
