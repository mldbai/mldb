/* stat_aggregator.cc
   Jeremy Banres, 3 August 2011
   Copyright (c) 2011 mldb.ai inc.  All rights reserved.

   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
*/

#include "mldb/soa/service/stat_aggregator.h"
#include "mldb/arch/exception.h"
#include "mldb/arch/format.h"
#include <iostream>
#include "mldb/jml/utils/floating_point.h"
#include "mldb/jml/utils/smart_ptr_utils.h"
#include "mldb/base/exc_check.h"
#include <algorithm>


using namespace std;
using namespace ML;

namespace MLDB {


/*****************************************************************************/
/* COUNTER AGGREGATOR                                                        */
/*****************************************************************************/

CounterAggregator::
CounterAggregator()
    : start(Date::now()), total(0.0),
      totalsBuffer() // Keep 10sec of data.
{
}

CounterAggregator::
~CounterAggregator()
{
}

void
CounterAggregator::
record(float value)
{
    double oldval = total;

    // No atomic ops for double, so we need to do it in a compare/exchange
    // loop.
    for (;;) {
        double newval = oldval + value;
        if (total.compare_exchange_weak(oldval, newval))
            return;
    }
}

std::pair<double, Date>
CounterAggregator::
reset()
{
    double oldval = total.exchange(0.0);

    Date oldStart = start;
    start = Date::now();

    return make_pair(oldval, oldStart);
}

std::vector<StatReading>
CounterAggregator::
read(const std::string & prefix)
{
    double current;
    Date oldStart;

    std::tie(current, oldStart) = reset();

    if (totalsBuffer.size() >= 10)
        totalsBuffer.pop_front();
    totalsBuffer.push_back(current);

    // Grab the average of the last x seconds to make sure that sparse values
    // show up in at least one of the x second carbon window. (x = 10 for now).
    double value = accumulate(totalsBuffer.begin(), totalsBuffer.end(), 0.0);
    value /= totalsBuffer.size();

    return vector<StatReading>(1, StatReading(prefix, value, start));
}


/*****************************************************************************/
/* GAUGE AGGREGATOR                                                          */
/*****************************************************************************/

GaugeAggregator::
GaugeAggregator(Verbosity verbosity, const std::vector<int>& extra)
    : verbosity(verbosity), values(new distribution<float>())
    , extra(extra)
{
    if (verbosity == Outcome)
        ExcCheck(this->extra.size() > 0, "Can not construct with empty percentiles");

    // OK, since nothing should access concurrently until the constructor
    // has returned.
    values.load()->reserve(100);
}

GaugeAggregator::
~GaugeAggregator()
{
    delete values.load();
}

void
GaugeAggregator::
record(float value)
{
    distribution<float> * current;
    while ((current = values) == 0
           || !values.compare_exchange_weak(current, nullptr));
    
    ExcCheck(current, "logic error in recording to gauge");
    current->push_back(value);
    
    values = current;
}

std::pair<distribution<float> *, Date>
GaugeAggregator::
reset()
{
    distribution<float> * current;
    distribution<float> * new_current = new distribution<float>();
    new_current->reserve(100);

    while ((current = values) == 0
           || !values.compare_exchange_weak(current, new_current));

    // Date oldStart = start;
    start = Date::now();

    return make_pair(current, start);
}

std::vector<StatReading>
GaugeAggregator::
read(const std::string & prefix)
{
    distribution<float> * values;
    Date oldStart;

    std::tie(values, oldStart) = reset();

    std::auto_ptr<distribution<float> > vptr(values);

    if (values->empty())
        return vector<StatReading>();
    
    vector<StatReading> result;

    auto addMetric = [&] (const char * name, double value)
        {
            result.push_back(StatReading(prefix + "." + name,
                                         value, start));
        };
    
    auto percentile = [&] (float outOf100) -> double
        {
            int element
                = std::max(0,
                           std::min<int>(values->size() - 1,
                                         outOf100 / 100.0 * values->size()));
            return (*values)[element];
        };
    
    std::sort(values->begin(), values->end(),
              ML::safe_less<float>());

    if (verbosity == StableLevel)
        result.push_back(StatReading(prefix, values->mean(), start));
    
    else {
        addMetric("mean", values->mean());
        addMetric("upper", values->back());
        addMetric("lower", values->front());

        if (verbosity == Outcome) {
            addMetric("count", values->size());
            for (int pct: extra) {
                addMetric(MLDB::format("upper_%d", pct).c_str(), percentile(pct));
            }
        }
    }

    return result;
}

} // namespace MLDB
