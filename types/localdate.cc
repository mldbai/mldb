// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* localdate.cc
   Wolfgang Sourdeau, 10 December 2012
   Copyright (c) 2012 mldb.ai inc.  All rights reserved.

*/


#include <libgen.h>
#include <time.h>
#include <mutex>
#include <boost/date_time/local_time/tz_database.hpp>
#include <boost/date_time/local_time/local_time.hpp>
#include <arch/exception.h>
#include "jml/utils/guard.h"
#include <sys/stat.h>


#include "date.h"
#include "localdate.h"

using namespace std;
using namespace boost::local_time;
using namespace boost::posix_time;


namespace {

static tz_database tz_db;
std::once_flag once;

string
dirName(const string & filename)
{
    char * fnCopy = ::strdup(filename.c_str());
    ML::Call_Guard guard([&]() { free(fnCopy); });
    char * dirNameC = ::dirname(fnCopy);
    string dirname(dirNameC);

    return dirname;
}

const boost::posix_time::ptime
epoch(boost::gregorian::date(1970, 1, 1));

} // file scope

namespace MLDB {

/*****************************************************************************/
/* DATE                                                                      */
/*****************************************************************************/

LocalDate::
LocalDate(double secondsSinceEpoch, const std::string & tzName)
    : secondsSinceEpoch_(secondsSinceEpoch), tzName_(tzName), tzOffset_(0)
{
    recomputeTZOffset();
}

double
LocalDate::
secondsSinceEpoch() const
{
    return secondsSinceEpoch_;
}

const string
LocalDate::
timezone() const
{
    return tzName_;
}

int
LocalDate::
tzOffset() const
{
    return tzOffset_;
}

void
LocalDate::
fillTM(struct tm & time) const
{
    time_t t = secondsSinceEpoch_ + tzOffset_;

    if (!gmtime_r(&t, &time))
        throw MLDB::Exception("problem with gmtime_r");
}

int
LocalDate::
hour() const
{
    tm time;

    fillTM(time);

    return time.tm_hour;
}

int
LocalDate::
dayOfMonth() const
{
    tm time;

    fillTM(time);

    return time.tm_mday;
}

int
LocalDate::
dayOfWeek() const
{
    tm time;

    fillTM(time);

    return time.tm_wday;
}

int
LocalDate::
year() const
{
    tm time;

    fillTM(time);

    return time.tm_year + 1900;
}

static std::string get_link_target(const std::string & link)
{
    /* Interface to the readlink call */
    size_t bufsize = 1024;
    
    /* Loop over, making the buffer successively larger if it is too small. */
    while (true) {  // break in loop
        char buf[bufsize];
        int res = readlink(link.c_str(), buf, bufsize);
        if (res == -1)
            throw MLDB::Exception(errno, "readlink", "get_link_name()");
        if (res == bufsize) {
            bufsize *= 2;
            continue;
        }
        buf[res] = 0;
        return buf;
    }
}

static bool fileExists(const std::string & filename)
{
    struct stat stats;
    int res = stat(filename.c_str(), &stats);
    if (res == -1)
        return false;  // file doesn't exist
    return true;
}

string
LocalDate::
findTimezoneSpec()
    const
{
    /* first, we attempt to load the csv from a path relative to the
       executable */
    string exeName = get_link_target("/proc/self/exe");

    /* the ".." entries enable this code to work from tests are well
       as from regular programs */
    string specFile = (dirName(exeName)
                       + "/../../../" LIB "/date_timezone_spec.csv");
    if (fileExists(specFile)) {
        return specFile;
    }

    /* second, we attempt to load from the current directory */
    specFile = LIB "/date_timezone_spec.csv";
    if (fileExists(specFile)) {
        return specFile;
    }

    throw MLDB::Exception("timezone spec file not found");    
}

void
LocalDate::
recomputeTZOffset()
{
    if (tzName_ == "UTC") {
        tzOffset_ = 0;
        // dstOffset_= 0;
    }
    else {
        call_once(once, [&] {
            string specFile = findTimezoneSpec();
            tz_db.load_from_file(specFile);
        });

        time_zone_ptr tz = tz_db.time_zone_from_region(tzName_);
        if (tz == 0) {
            throw MLDB::Exception("time zone named '" + tzName_ + "' is not known");
        }
        time_duration offset = tz->base_utc_offset();
        tzOffset_ = offset.total_seconds();

        if (tz->has_dst()) {
            int yearOfLocalDate = year();
            int dst_start((tz->dst_local_start_time(yearOfLocalDate) - epoch)
                          .total_seconds());
            int dst_end((tz->dst_local_end_time(yearOfLocalDate) - epoch)
                        .total_seconds());

            // the southern areas of the world have their DST during northern
            // winter
            if (dst_start < dst_end) {
                if (secondsSinceEpoch_ >= dst_start
                    && dst_end > secondsSinceEpoch_)
                    tzOffset_ += tz->dst_offset().total_seconds();
            }
            else {
                if (secondsSinceEpoch_ >= dst_end
                    && dst_start > secondsSinceEpoch_)
                    tzOffset_ += tz->dst_offset().total_seconds();
            }
        }
    }

    // cerr << "timezone: " << tzName_
    //      << "; seconds: " << secondsSinceEpoch_
    //      << "; offset: " << tzOffset_
    //      << "; dst offset: " << dstOffset_
    //      << endl;
}

} // namespace MLDB
