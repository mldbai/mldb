// -*- C++ -*-
// em.h
// Mathieu Marquis Bolduc - 30 Dec 2015
// Copyright (c) 2013 mldb.ai inc. All rights reserved.
// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

#pragma once

#include "mldb/jml/stats/distribution.h"
#include <boost/multi_array.hpp>
#include "mldb/jml/db/persistent.h"
#include "mldb/vfs/filter_streams.h"
#include "mldb/types/string.h"

namespace ML {

struct EstimationMaximisation
{
    struct Cluster {
        double totalWeight;
        distribution<double> centroid;
        boost::multi_array<double, 2> covarianceMatrix;
        boost::multi_array<double, 2> invertCovarianceMatrix;
        double pseudoDeterminant;
    };

    std::vector<Cluster> clusters;
    std::vector<MLDB::Utf8String> columnNames;

    void
    train(const std::vector<distribution<double>> & points,
          std::vector<int> & in_cluster,
          int nbClusters,
          int maxIterations,
          int randomSeed); 

    int
    assign(const distribution<double> & point,
           boost::multi_array<double, 2>& distanceMatrix,
           int pIndex) const;
    int
    assign(const distribution<double> & point) const;
  
    void serialize(ML::DB::Store_Writer & store) const;
    void reconstitute(ML::DB::Store_Reader & store);
    void save(const std::string & filename) const;
    void load(const std::string & filename);

};

} // namespace ML
