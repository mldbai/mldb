// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* event_service.cc
   Jeremy Barnes, 29 May 2012
   Copyright (c) 2012 mldb.ai inc.  All rights reserved.

   High frequency logging service.
*/

#include "event_service.h"
#include "multi_aggregator.h"
#include <iostream>
#include "mldb/arch/demangle.h"
#include "mldb/base/exc_assert.h"
#include "mldb/jml/utils/environment.h"
#include "mldb/jml/utils/file_functions.h"
#include <unistd.h>
#include <sys/utsname.h>

#include "mldb/ext/jsoncpp/reader.h"
#include "mldb/ext/jsoncpp/value.h"
#include <fstream>
#include <sys/utsname.h>

using namespace std;

extern const char * __progname;

namespace MLDB {

/*****************************************************************************/
/* EVENT SERVICE                                                             */
/*****************************************************************************/

std::map<std::string, double>
EventService::
get(std::ostream & output) const {
    std::map<std::string, double> result;

    std::stringstream ss;
    dump(ss);

    while (ss)
    {
        string line;
        getline(ss, line);
        if (line.empty()) continue;

        size_t pos = line.rfind(':');
        ExcAssertNotEqual(pos, string::npos);
        string key = line.substr(0, pos);

        pos = line.find_first_not_of(" \t", pos + 1);
        ExcAssertNotEqual(pos, string::npos);
        double value = stod(line.substr(pos));
        result[key] = value;
    }

    output << ss.str();
    return result;
}

/*****************************************************************************/
/* NULL EVENT SERVICE                                                        */
/*****************************************************************************/

NullEventService::
NullEventService()
    : stats(new MultiAggregator())
{
}

NullEventService::
~NullEventService()
{
}

void
NullEventService::
onEvent(const std::string & name,
        const char * event,
        EventType type,
        float value,
        std::initializer_list<int>)
{
    stats->record(name + "." + event, type, value);
}

void
NullEventService::
dump(std::ostream & stream) const
{
    stats->dumpSync(stream);
}


/*****************************************************************************/
/* EVENT RECORDER                                                            */
/*****************************************************************************/

EventRecorder::
EventRecorder(const std::string & eventPrefix,
              const std::shared_ptr<EventService> & events)
    : eventPrefix_(eventPrefix),
      events_(events)
{
}

void
EventRecorder::
recordEvent(const char * eventName,
            EventType type,
            float value,
            std::initializer_list<int> extra) const
{
    EventService * es = 0;
    if (events_)
        es = events_.get();
    if (!es)
        {
            std::cerr << "no services configured!!!!" << std::endl;
            return;
        }
    es->onEvent(eventPrefix_, eventName, type, value, extra);
}

void
EventRecorder::
recordEventFmt(EventType type,
               float value,
               std::initializer_list<int> extra,
               const char * fmt, ...) const
{
    if (!events_)  return;
        
    char buf[2048];
        
    va_list ap;
    va_start(ap, fmt);
    try {
        int res = vsnprintf(buf, 2048, fmt, ap);
        if (res < 0)
            throw MLDB::Exception("unable to record hit with fmt");
        if (res >= 2048)
            throw MLDB::Exception("key is too long");
            
        recordEvent(buf, type, value);
        va_end(ap);
        return;
    }
    catch (...) {
        va_end(ap);
        throw;
    }
}

} // namespace MLDB
