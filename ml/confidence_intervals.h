// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* -*- C++ -*-
   confidence_intervals.h
   Copyright (c) 2011 mldb.ai inc.  All rights reserved.
*/

#pragma once

#include <utility>
#include <vector>
#include <string>
#include <functional>
#include "mldb/jml/db/persistent.h"

namespace MLDB
{
    class ConfidenceIntervals
    {
    private:
        typedef enum {
            UPPER=1,
            LOWER=-1
        } WilsonBoundDirection;

        typedef enum {
            WILSON,
            CLOPPER_PEARSON
        } Method;


        std::function<double(double, double, double)> wilsonFnct;

        Method method;
        double alpha_;
        std::vector<double> createBootstrapSamples(const std::vector<double>& sample, int replications,
                int resampleSize) const;

        double wilsonBinomialUpperLowerBound(int trials, int successes, WilsonBoundDirection dir) const;
        void assertClopperPearson() const;
        
        void init(std::string method);
        std::string print(Method m) const;
    public:
        ConfidenceIntervals(float alpha, std::string m="clopper_pearson");
        unsigned getMethod();

        double binomialUpperBound(int trials, int successes) const;
        double binomialLowerBound(int trials, int successes) const;
        std::pair<double,double> binomialTwoSidedBound(int trials, int successes) const;

        double bootstrapMeanUpperBound(const std::vector<double>& sample, int replications,
                int resampleSize) const;
        double bootstrapMeanLowerBound(const std::vector<double>& sample, int replications,
                int resampleSize) const;
        std::pair<double,double> bootstrapMeanTwoSidedBound(const std::vector<double>& sample, int replications,
                int resampleSize) const;
        
        void serialize(ML::DB::Store_Writer & store) const;
        void reconstitute(ML::DB::Store_Reader & store);
    };
}

