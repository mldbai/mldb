
// This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

// -*- C++ -*-
// em.h
// Mathieu Marquis Bolduc - 30 Dec 2015
// Copyright (c) 2013 Datacratic. All rights reserved.
//

#pragma once

#include "mldb/jml/stats/distribution.h"
#include <boost/multi_array.hpp>
#include "mldb/ml/Eigen/Dense"

namespace ML {

struct EstimationMaximisation
{
    struct Cluster {
        double totalWeight;
        ML::distribution<float> centroid;
        Eigen::MatrixXd covarianceMatrix;
        Eigen::MatrixXd invertCovarianceMatrix;
        float pseudoDeterminant;
    };

    std::vector<Cluster> clusters;

  void
  train(const std::vector<ML::distribution<float>> & points,
        std::vector<int> & in_cluster,
        int nbClusters,
        int maxIterations,
        int randomSeed); 

  int
  assign(const ML::distribution<float> & point, Eigen::MatrixXd& distanceMatrix, int pIndex) const;
  

};

} // ML