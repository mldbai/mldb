
// This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

// -*- C++ -*-
// em.h
// Mathieu Marquis Bolduc - 30 Dec 2015
// Copyright (c) 2013 Datacratic. All rights reserved.
//

#pragma once

#include "mldb/jml/stats/distribution.h"
#include <boost/multi_array.hpp>

namespace ML {

struct EstimationMaximisation
{
    struct Cluster {
        double totalWeight;
        ML::distribution<double> centroid;
        boost::multi_array<double, 2> covarianceMatrix;
        boost::multi_array<double, 2> invertCovarianceMatrix;
        double pseudoDeterminant;
    };

    std::vector<Cluster> clusters;

  void
  train(const std::vector<ML::distribution<double>> & points,
        std::vector<int> & in_cluster,
        int nbClusters,
        int maxIterations,
        int randomSeed); 

  int
  assign(const ML::distribution<double> & point, boost::multi_array<double, 2>& distanceMatrix, int pIndex) const;
  

};

} // ML
