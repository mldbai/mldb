// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/*
   confidence_intervals.cc
   Copyright (c) 2011 mldb.ai inc.  All rights reserved.
*/

#include "confidence_intervals.h"
#include <boost/math/distributions/binomial.hpp>
#include <boost/math/distributions/normal.hpp> // for normal_distribution
#include "mldb/ml/jml/thread_context.h"
#include <math.h>
#include "mldb/arch/exception.h"


using namespace std;
using namespace boost;
using boost::math::normal; // typedef provides default type is double.

namespace MLDB
{

ConfidenceIntervals::
ConfidenceIntervals(float alpha, std::string m) : alpha_(alpha) {
    init(m);
}

void ConfidenceIntervals::init(std::string m) {
    if(m=="wilson") {
        method = WILSON;

        // Construct a standard normal distribution s
        normal s; // (default mean = zero, and standard deviation = unity)
        const double b = quantile(s, 1-alpha_);
        const double d = pow(b, 2);
        const double a = d / 2;
        const double c = d / 4;

        wilsonFnct = [=] (double sumNum, double sumDenom, double sign)
            {
                return (a/sumDenom+sumNum/sumDenom+b*
                        sign*sqrt(c/(sumDenom*sumDenom)+(sumNum*
                        (1-sumNum/sumDenom))/(sumDenom*sumDenom)))/(1+d/sumDenom);
            };
    }
    else if (m=="clopper_pearson") {
        method = CLOPPER_PEARSON;
    }
    else {
        throw MLDB::Exception(MLDB::format("Unknown confidence interval method '%s'", m.c_str()));
    }
}

unsigned
ConfidenceIntervals::getMethod()
{
    return method;
}

double
ConfidenceIntervals::
wilsonBinomialUpperLowerBound(int trials,
        int successes, WilsonBoundDirection dir) const
{
    double sumNum = (double)successes;
    double sumDenom = (double)trials;
    double sign = (double)dir;
    return wilsonFnct(sumNum, sumDenom, sign);
}

double
ConfidenceIntervals::
binomialUpperBound(int trials, int successes) const
{
    switch(method){
    case WILSON:          return wilsonBinomialUpperLowerBound(trials, successes, UPPER);
    case CLOPPER_PEARSON: return math::binomial_distribution<>::find_upper_bound_on_p(trials, successes, alpha_);
    }
    throw MLDB::Exception(MLDB::format("Unknown method '%s'", method));
}

double
ConfidenceIntervals::
binomialLowerBound(int trials, int successes) const
{
    switch(method){
    case WILSON:          return wilsonBinomialUpperLowerBound(trials, successes, LOWER);
    case CLOPPER_PEARSON: return math::binomial_distribution<>::find_lower_bound_on_p(trials, successes, alpha_);
    }
    throw MLDB::Exception(MLDB::format("Unknown method '%s'", method));
}

pair<double,double>
ConfidenceIntervals::
binomialTwoSidedBound(int trials, int successes) const
{
    assertClopperPearson();
    return make_pair(
            math::binomial_distribution<>::find_lower_bound_on_p(trials, successes, alpha_/2.0),
            math::binomial_distribution<>::find_upper_bound_on_p(trials, successes, alpha_/2.0)
        );
}

vector<double>
ConfidenceIntervals::
createBootstrapSamples(const vector<double>& sample, int replications,
        int resampleSize) const
{
    assertClopperPearson();
    int sampleSize = sample.size();

    if(sampleSize ==0)
    {
        throw MLDB::Exception("Can't compute bootstrap mean from empty sample");
    }

    ML::Thread_Context rng;

    vector<double> resampleMeans;
    for(int i=0; i<replications;i++)
    {
        double accumulator = 0;
        for(int j=0;j<resampleSize;j++)
        {
            accumulator += sample[rng.random() % sampleSize];
        }
        resampleMeans.push_back(accumulator/resampleSize);
    }
    sort(resampleMeans.begin(), resampleMeans.end());
    return resampleMeans;
}

double
ConfidenceIntervals::
bootstrapMeanUpperBound(const vector<double>& sample, int replications,
        int resampleSize) const
{
    assertClopperPearson();
    vector<double> resampleMeans = createBootstrapSamples(sample, replications, resampleSize);
    return resampleMeans[replications*(1-alpha_)];
}

double
ConfidenceIntervals::
bootstrapMeanLowerBound(const vector<double>& sample, int replications,
        int resampleSize) const
{
    assertClopperPearson();
    vector<double> resampleMeans = createBootstrapSamples(sample, replications, resampleSize);
    return resampleMeans[replications*alpha_];
}

pair<double,double>
ConfidenceIntervals::
bootstrapMeanTwoSidedBound(const vector<double>& sample, int replications,
        int resampleSize) const
{
    assertClopperPearson();
    vector<double> resampleMeans = createBootstrapSamples(sample, replications, resampleSize);
    return make_pair(
            resampleMeans[replications*(alpha_/2.0)],resampleMeans[replications*(1-alpha_/2.0)]
        );
}

void ConfidenceIntervals::assertClopperPearson() const
{
    if (method != CLOPPER_PEARSON)
        throw MLDB::Exception("Can only use this method with Clopper-Peason method!");
}
        
std::string ConfidenceIntervals::
print(Method m) const
{
    switch(m) {
    case WILSON:          return "wilson";
    case CLOPPER_PEARSON: return "clopper_pearson";
    default:
        throw MLDB::Exception("Unknown method");
    }
}

void ConfidenceIntervals::
serialize(ML::DB::Store_Writer & store) const
{
    int version = 1;
    store << version << print(method) << alpha_;
}

void ConfidenceIntervals::
reconstitute(ML::DB::Store_Reader & store)
{
    int version;
    int REQUIRED_V = 1;
    store >> version;
    if(version!=REQUIRED_V) {
        throw MLDB::Exception(MLDB::format(
                    "invalid ConfidenceInterval version! exptected %d, got %d", 
                    REQUIRED_V, version));
    }

    string method;
    store >> method >> alpha_;
    init(method);
}

}

