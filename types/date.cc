// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* date.cc
   Jeremy Barnes, 18 July 2010
   Copyright (c) 2010 mldb.ai inc.  All rights reserved.

*/

#include "date.h"
#include "date_description.h"
#include <cmath>
#include <limits>
#include "mldb/arch/format.h"
#include "mldb/ext/jsoncpp/json.h"
#include "mldb/base/parse_context.h"
#include <cmath>
#include "mldb/arch/exception.h"
#include "dtoa.h"
#include <chrono>
#include <boost/regex.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>



using namespace std;
using namespace MLDB;

namespace {

bool
matchFixedWidthInt(ParseContext & context,
                   int minLength, int maxLength,
                   int min, int max, int & value)
{
    ParseContext::Revert_Token token(context);

    char buf[maxLength + 1];
    unsigned i = 0;
    for (;  i < maxLength && context;  ++i, ++context) {
        char c = *context;
        if (c < '0' || c > '9') {
            if (i >= minLength)
                break;
            else
                return false;
        }
        buf[i] = c;
    }
    if (i < minLength)
        return false;
    buf[i] = 0;

    char * endptr = 0;
    errno = 0;
    int result = strtol(buf, &endptr, 10);
    
    if (errno || *endptr != 0 || endptr != buf + i)
        context.exception("expected fixed width int");

    // This WILL bite us some time.  640k anyone?
    if (result < min || result > max) {
        return false;
    }

    token.ignore();

    value = result;

    return true;
}

int
expectFixedWidthInt(ParseContext & context,
                    int minLength, int maxLength,
                    int min, int max, const char * message)
{
    int result;

    if (!matchFixedWidthInt(context, minLength, maxLength,
                            min, max, result)) {
        context.exception(message);
    }

    return result;
}

const boost::posix_time::ptime
epoch(boost::gregorian::date(1970, 1, 1));

}

namespace MLDB {


/*****************************************************************************/
/* DATE                                                                      */
/*****************************************************************************/

Date::
Date(int year, int month, int day,
     int hour, int minute, int second,
     double fraction)
    : secondsSinceEpoch_((boost::posix_time::ptime
                            (boost::gregorian::date(year, month, day)) - epoch)
                           .total_seconds()
                           + 3600 * hour + 60 * minute + second
                           + fraction)
{
}

Date::
Date(const Json::Value & value)
{
    if (value.isConvertibleTo(Json::realValue)) {
        secondsSinceEpoch_ = value.asDouble();
    }
    else if (value.isConvertibleTo(Json::stringValue)) {
        Date parsed = parse_date_time(value.asString(), "%y-%M-%d", "%H:%M:%S");
        secondsSinceEpoch_ = parsed.secondsSinceEpoch_;
    }
    else throw Exception("Date::Date(Json): JSON value "
                         + value.toStyledString()
                         + "not convertible to date");
}

Date
Date::
fromIso8601Week(int year, int week, int day)
{
    Date newDate(year, 1, 1);

    int currentWeek = newDate.iso8601WeekOfYear();
    if (currentWeek == 1) {
        newDate.addWeeks(week - 1);
    }
    else {
        newDate.addWeeks(week);
    }

    return newDate.iso8601WeekStart().plusDays(day - 1);
}

Date
Date::
parseSecondsSinceEpoch(const std::string & date)
{
    errno = 0;
    char * end = 0;
    double seconds = strtod(date.c_str(), &end);
    if (errno != 0)
        throw MLDB::Exception(errno, "date parseSecondsSinceEpoch: " + date);
    if (end != date.c_str() + date.length())
        throw MLDB::Exception("couldn't convert " + date + " to date");
    return fromSecondsSinceEpoch(seconds);
}

Date
Date::
parseDefaultUtc(const std::string & date)
{
    if (date == "NaD" || date == "NaN")
        return notADate();
    else if (date == "Inf")
        return positiveInfinity();
    else if (date == "-Inf")
        return negativeInfinity();

    return parse_date_time(date, "%y-%M-%d", "%H:%M:%S");
}

Date
Date::
parseIso8601(const std::string & date)
{
    if (date == "NaD" || date == "NaN")
        return notADate();
    else if (date == "Inf")
        return positiveInfinity();
    else if (date == "-Inf")
        return negativeInfinity();
    else {
        return parse_date_time(date, "%y-%m-%d", "T%H:%M:%SZ");
    }
}

Date
Date::
parseIso8601DateTime(const std::string & dateTimeStr)
{
    if (dateTimeStr == "NaD" || dateTimeStr == "NaN")
        return notADate();
    else if (dateTimeStr == "Inf")
        return positiveInfinity();
    else if (dateTimeStr == "-Inf")
        return negativeInfinity();
    else {
        Date date;
        Iso8601Parser parser(dateTimeStr);
        if (!parser.matchDateTime(date))
            return notADate();
        return date;
    }
}

Date
Date::
notADate()
{
    Date result;
    result.secondsSinceEpoch_ = std::numeric_limits<double>::quiet_NaN();
    return result;
}

Date
Date::
positiveInfinity()
{
    Date result;
    result.secondsSinceEpoch_ = INFINITY;
    return result;
}

Date
Date::
negativeInfinity()
{
    Date result;
    result.secondsSinceEpoch_ = -INFINITY;
    return result;
}

Date
Date::
now()
{
    timespec time;
    int res = clock_gettime(CLOCK_REALTIME, &time);
    if (res == -1)
        throw MLDB::Exception(errno, "clock_gettime");
    return fromSecondsSinceEpoch(time.tv_sec + time.tv_nsec * 0.000000001);
}

bool
Date::
isADate() const
{
    return std::isfinite(secondsSinceEpoch_);
}

/** Add the fractional seconds on to the timestamp.  Returns a string which is
    - empty if seconds_digits is 0
    - .xxx where seconds_digits > 0, for the given number of digits
    - .xxx or nothing where seconds_digits = -1, in such a way that the number is
      represented exactly with the minimum number of digits.

      We pass in the full seconds since the epoch, to allow us to faithfully
      record the number in such a way that the full precision of the value in
      the double is recorded.
*/
static void addFractionalSeconds(std::string & result,
                                 double full_seconds,
                                 int seconds_digits)
{
    if (seconds_digits == 0)
        return;

    //cerr << "adding " << seconds_digits << " to " << result << " with "
    //     << full_seconds << " partial seconds" << endl;

    //cerr << "total seconds are " << jsonEncodeStr(full_seconds) << endl;
    if (seconds_digits == -1) {
        int decpt;
        int sign;

        char * fractionalPtr = soa_dtoa(full_seconds, 1, -1 /* ndigits */,
                                        &decpt, &sign, nullptr);
        std::string fractional(fractionalPtr);
        soa_freedtoa(fractionalPtr);

        while (decpt < 0) {
            fractional = '0' + fractional;
            ++decpt;
        }

        //std::string fractional = MLDB::dtoa(full_seconds);
        //cerr << "fractional = " << fractional << endl;
        //cerr << "decpt = " << decpt << endl;
        //cerr << "sign = " << sign << endl;

        if (decpt >= fractional.length() || fractional == "0")
            return;  // no extra digits

        result += '.';
        

        //auto dotPos = fractional.find('.');

        // Remove the whole seconds part and append
        result.append(fractional, decpt, -1);
    }
    else if (seconds_digits > 0) {
        
        double whole_seconds;
        double partial_seconds = modf(full_seconds >= 0
                                      ? full_seconds : -full_seconds,
                                      &whole_seconds);
        
        string fractional = format("%.*f", seconds_digits, partial_seconds);

        if (fractional == "0.0")
            return;
        // Remove the leading "0" and append
        result.append(fractional, 1, -1);
    }
    else throw MLDB::Exception("Unknown seconds_digits argument %d to Date::printIso8601()",
                             seconds_digits);
}

std::string
Date::
print(int seconds_digits) const
{
    if (!std::isfinite(secondsSinceEpoch_)) {
        if (std::isnan(secondsSinceEpoch_)) {
            return "NaD";
        }
        else if (secondsSinceEpoch_ > 0) {
            return "Inf";
        }
        else return "-Inf";
    }

    string result = print("%Y-%b-%d %H:%M:%S");

    if (result == "Inf" || result == "-Inf" || result == "NaD")
        return result;

    if (seconds_digits == 0) return result;

    addFractionalSeconds(result, secondsSinceEpoch(), seconds_digits);

    return result;
}

std::string
Date::
printRfc2616() const
{
    if (!std::isfinite(secondsSinceEpoch_)) {
        if (std::isnan(secondsSinceEpoch_)) {
            return "NaD";
        }
        else if (secondsSinceEpoch_ > 0) {
            return "Inf";
        }
        else return "-Inf";
    }

    return print("%a, %d %b %Y %H:%M:%S GMT");
}

std::string
Date::
printIso8601(int seconds_digits) const
{
    if (!std::isfinite(secondsSinceEpoch_)) {
        if (std::isnan(secondsSinceEpoch_)) {
            return "NaD";
        }
        else if (secondsSinceEpoch_ > 0) {
            return "Inf";
        }
        else return "-Inf";
    }

    string result = print("%Y-%m-%dT%H:%M:%S");

    if (result == "Inf" || result == "-Inf" || result == "NaD")
        return result;
    
    addFractionalSeconds(result, secondsSinceEpoch(), seconds_digits);

    result += "Z";
    return result;
}

std::string
Date::
printClassic() const
{
    if (!std::isfinite(secondsSinceEpoch_)) {
        if (std::isnan(secondsSinceEpoch_)) {
            return "NaD";
        }
        else if (secondsSinceEpoch_ > 0) {
            return "Inf";
        }
        else return "-Inf";
    }

    return print("%Y-%m-%d %H:%M:%S");
}

Date
Date::
quantized(double fraction) const
{
    Date result = *this;
    return result.quantize(fraction);
}

Date &
Date::
quantize(double fraction)
{
    if (fraction <= 0.0)
        throw Exception("Date::quantize(): "
                        "fraction cannot be zero or negative");

    if (fraction <= 1.0) {
        // Fractions of a second; split off to avoid loss of precision
        double whole_seconds, partial_seconds;
        partial_seconds = modf(secondsSinceEpoch_, &whole_seconds);

        double periods_per_second = round(1.0 / fraction);
        partial_seconds = round(partial_seconds * periods_per_second)
            / periods_per_second;

        secondsSinceEpoch_ = whole_seconds + partial_seconds;
    }
    else {
        // Fractions of a second; split off to avoid loss of precision
        double whole_seconds;
        // double partial_seconds = modf(secondsSinceEpoch_, &whole_seconds);
        modf(secondsSinceEpoch_, &whole_seconds);

        uint64_t frac = fraction;
        if (frac != fraction)
            throw MLDB::Exception("non-integral numbers of seconds not supported");
        uint64_t whole2 = whole_seconds;
        whole2 /= fraction;
        secondsSinceEpoch_ = whole2 * fraction;
    }

    return *this;
}

std::string
Date::
print(const std::string & format) const
{
    size_t buffer_size = format.size() + 1024;
    char buffer[buffer_size];

    if (secondsSinceEpoch() >= 100000000000) {
        return "Inf";
    }
    if (secondsSinceEpoch() <= -1000000000000) {
        return "-Inf";
    }

    time_t t = secondsSinceEpoch();
    tm time;

    if (!gmtime_r(&t, &time)) {
        cerr << strerror(errno) << endl;
        cerr << t << endl;
        cerr << secondsSinceEpoch() << endl;
        throw Exception("problem with gmtime_r");
    }
    size_t nchars = strftime(buffer, buffer_size, format.c_str(),
                             &time);
    
    if (nchars == 0)
        throw Exception("couldn't print date format " + format);
    
    return string(buffer, buffer + nchars);
}

int
Date::
hour() const
{
    time_t t = secondsSinceEpoch();
    tm time;

    if (!gmtime_r(&t, &time))
        throw Exception("problem with gmtime_r");

    return time.tm_hour;
}

int
Date::
minute() const
{
    time_t t = secondsSinceEpoch();
    tm time;

    if (!gmtime_r(&t, &time))
        throw Exception("problem with gmtime_r");

    return time.tm_min;
}

int
Date::
second() const
{
    time_t t = secondsSinceEpoch();
    tm time;

    if (!gmtime_r(&t, &time))
        throw Exception("problem with gmtime_r");

    return time.tm_sec;
}

int
Date::
millisecond() const
{
    double fractional = fractionalSeconds();
    return (int)(floor(fractional * 1000.0f));
}

int
Date::
microsecond() const
{
    double fractional = fractionalSeconds();
    return (int)(floor(fractional * 1000000.0f));
}

int
Date::
weekday()
    const
{
    using namespace boost::gregorian;

    int day_of_week;
    try {
        day_of_week = from_string(print()).day_of_week();
    } catch (...) {
        cerr << "order_date = " << *this << endl;
        throw;
    }

    return day_of_week;
}

int
Date::
iso8601Weekday()
    const
{
    //starts on monday with number 1
    return ((weekday() + 6) % 7) + 1; 
}

int
Date::
dayOfMonth() const
{
    return boost::gregorian::from_string(print()).day();
}

int
Date::
dayOfYear()
    const
{
    time_t t = secondsSinceEpoch_;
    struct tm time;

    ::gmtime_r(&t, &time);

    return time.tm_yday;
}

int 
Date::
iso8601DayofYear() const
{
    //how many days since the day 1 of week 1. + 1.
    return (iso8601WeekOfYear() - 1) * 7 + iso8601Weekday();
}

int
Date::
iso8601WeekOfYear()
    const
{
    //Jan 4 is always in week 1
    //week 1 has the first thursday of the year in it

    Date today(year(), monthOfYear(), dayOfMonth()); //to wipe the hour

    //move to the tursday of this iso week
    //to get the reference year
    Date thursday = today;
    thursday.addDays(4-today.iso8601Weekday());

    //get the first week of the iso year
    //start with jan 1st
    Date firstweek = Date(thursday.year(), 1, 1);
    int firstweekIsoWeekDay = firstweek.iso8601Weekday();

    //get the monday. if Fri/sat/sun get next monday 
    if (firstweekIsoWeekDay > 4) //adjust forward
    {
        firstweek.addDays(8-firstweekIsoWeekDay);
    }
    else
    {
        firstweek.addDays(-firstweekIsoWeekDay + 1);
    }

    //how many full weeks since the monday of the first iso week of our iso year
    double diff = today.secondsSinceEpoch() - firstweek.secondsSinceEpoch() ;

    return (int)floor(diff / 604800) + 1; 

}

int
Date::
iso8601Year() const
{
    int nyear = year();
    int month = monthOfYear();

    int isoWeek = iso8601WeekOfYear();

    if (isoWeek == 1 && month == 12)
    {
        //week 1 in december -- as far as iso is concerned, we're already next year
        ++nyear;
    }
    else if (month == 1 && isoWeek > 5)
    {
        //late week in january -- as far as iso is concerned, we're still last year
        --nyear;
    }

    return nyear;
}

int
Date::
monthOfYear() const
{
    return boost::gregorian::from_string(print()).month();
}
int 
Date::
week() const
{
    return (dayOfYear() / 7);
}

int
Date::
quarter() const
{
    return (monthOfYear() / 4) + 1;
}

int
Date::
year() const
{
    return boost::gregorian::from_string(print()).year();
}

int
Date::
hourOfWeek() const
{
    time_t t = secondsSinceEpoch();
    tm time;

    if (!gmtime_r(&t, &time))
        throw Exception("problem with gmtime_r");

    return time.tm_wday * 24 + time.tm_hour;
}

int 
Date::
get(TimeUnit unit) const
{
    switch (unit)
    {
        case MICROSECOND:
            return microsecond();
        case MILLISECOND:
            return millisecond();
        case SECOND:
            return second();
        case MINUTE:
            return minute();
        case HOUR:
            return hour();
        case DAY:
            return dayOfMonth();
        case DOW:
            return weekday();
        case DOY:
            return dayOfYear();
        case ISODOW:
            return iso8601Weekday();
        case ISODOY:
            return iso8601DayofYear();
        case WEEK:
            return week();
        case ISOWEEK:
            return iso8601WeekOfYear();
        case MONTH:
            return monthOfYear();
        case QUARTER:
            return quarter();
        case YEAR:
            return year();
        case ISOYEAR:
            return iso8601Year();
        case TIMEUNIT_INVALID:
            throw Exception("Invalid time unit");
    };

    throw Exception("Invalid time unit");

    return 0;
}

Date 
Date::
trunc(TimeUnit unit) const
{
    Date copy(*this);
    switch (unit)
    {
        case MICROSECOND:
        {
            double fractional = fractionalSeconds();
            double adjusted = floor(fractional*1000000.0f)/1000000.0f;

            copy.addSeconds( adjusted - fractional );
            break;
        }
        case MILLISECOND:
        {
            double fractional = fractionalSeconds();
            double adjusted = floor(fractional*1000.0f)/1000.0f;

            copy.addSeconds( adjusted - fractional );
            break;
        }
        case SECOND:
        {
            copy = Date::fromSecondsSinceEpoch(floor(secondsSinceEpoch_));
            break;
        }
        case MINUTE:
        {
            copy = Date::fromSecondsSinceEpoch(floor(secondsSinceEpoch_ / 60.0f)*60.0f);
            break;
        }
        case HOUR:
        {
            copy = Date::fromSecondsSinceEpoch(floor(secondsSinceEpoch_ / 3600.0f)*3600.0f);
            break;
        }
        case DOY:
        case DOW:
        case DAY:
        case ISODOW:
        case ISODOY:
        {
            copy = Date::fromSecondsSinceEpoch(floor(secondsSinceEpoch_ / 86400.0f)*86400.0f);
            break;
        }
        case WEEK:
        {
            copy = Date::fromSecondsSinceEpoch(floor(secondsSinceEpoch_ / 86400.0f)*86400.0f);
            int day = copy.weekday(); //0-6
            copy.addSeconds( -(day*86400.0f) );
            break;
        }
        case ISOWEEK:
        {
            copy = Date::fromSecondsSinceEpoch(floor(secondsSinceEpoch_ / 86400.0f)*86400.0f);
            int day = copy.iso8601Weekday() - 1; //1-7
            copy.addSeconds( -(day*86400.0f) );
            break;
        }
        case MONTH:
        {
            copy = Date(year(), monthOfYear(), 1);
            break;
        }
        case QUARTER:
        {
            int month = (quarter() - 1)*3;
            copy = Date(year(), month, 1);
            break;
        }
        case YEAR:
        {
            copy = Date(year(), 1, 1);
            break;
        }
        case ISOYEAR:
        {
            copy = Date::fromSecondsSinceEpoch(floor(secondsSinceEpoch_ / 86400.0f)*86400.0f);
            int days = iso8601DayofYear() - 1; // 1 - 371 or 364
            copy.addSeconds( -(days*86400.0f) );

            break;
        }
        case TIMEUNIT_INVALID:
        default:
            throw Exception("Invalid time unit");
    };

    return copy;
}

Date Date::plusMonthDaySecond(int64_t months, int64_t days, float seconds) const
{
    double totalSeconds = secondsSinceEpoch_ + seconds; //get the seconds out of the way
    double intPart = 0;
    double fractional = modf(totalSeconds, &intPart);

    time_t t = intPart;    

    tm time;

    if (!gmtime_r(&t, &time))
        throw Exception("problem with gmtime_r");

    boost::gregorian::date gdate = boost::gregorian::date_from_tm(time);    

    gdate += boost::gregorian::date_duration(days);
    gdate += boost::gregorian::months(months);
    //convert back to epoch
    return Date(gdate.year(), gdate.month(), gdate.day(), time.tm_hour, time.tm_min, time.tm_sec + fractional); 
}

Date Date::minusMonthDaySecond(int64_t months, int64_t days, float seconds) const
{
    double totalSeconds = secondsSinceEpoch_ - seconds; //get the seconds out of the way
    double intPart = 0;
    double fractional = modf(totalSeconds, &intPart);

    time_t t = intPart;  

    tm time;

    if (!gmtime_r(&t, &time))
        throw Exception("problem with gmtime_r");

    boost::gregorian::date gdate = boost::gregorian::date_from_tm(time);    
    gdate -= boost::gregorian::date_duration(days);
    gdate -= boost::gregorian::months(months);

    //convert back to epoch
    return Date(gdate.year(), gdate.month(), gdate.day(), time.tm_hour, time.tm_min, time.tm_sec + fractional);   
}

std::tuple<int64_t, float>
Date::getDaySecondInterval(const Date& rDate) const
{
    //Even boost::gregorian only supports time differences in days 
    //So we approximate here

    double secondsDiff = secondsSinceEpoch_ - rDate.secondsSinceEpoch_;
    double absSecondsDiff = fabs(secondsDiff);
    const double secondsInADays = 60.0f*60.0f*24.0f;
    int64_t days = (int64_t)(absSecondsDiff / secondsInADays);
    absSecondsDiff -= days * secondsInADays;
    float seconds = copysign(absSecondsDiff, secondsDiff);

    return make_tuple(days, seconds);
}

std::string
Date::
printMonth() const
{
    throw Exception("Date: stub method");
}

std::string
Date::
printWeekday() const
{
    throw Exception("Date: stub method");
}

std::string
Date::
printYearAndMonth() const
{
    throw Exception("Date: stub method");
}

std::ostream & operator << (std::ostream & stream, const Date & date)
{
    return stream << date.print();
}

std::istream & operator >> (std::istream & stream, Date & date)
{
    throw Exception("date istream parsing doesn't work yet");
}

bool
Date::
match_date(ParseContext & context,
           Date & result,
           const std::string & format)
{
    ParseContext::Revert_Token token(context);
    
    int year = -1, month = 1, day = 1;
    
    for (const char * f = format.c_str();  context && *f;  ++f) {
        if (*f != '%') {
            if (!context.match_literal(*f)) return false;
            continue;
        }
        
        ++f;
        switch (*f) {
        case '%':
            if (!context.match_literal('%')) return false;
            break;
        case 'd':
            if (!context.match_int(day, 1, 31)) return false;
            break;
        case 'm':
            if (!context.match_int(month, 1, 12)) return false;
            break;
        case 'M':
            switch(tolower(*context)) {
            case '0':
            case '1':
            case '2':
            case '3':
            case '4':
            case '5':
            case '6':
            case '7':
            case '8':
            case '9': {
                if (!context.match_int(month, 1, 12))
                    return false;
                break;
            }
            case 'j': {
                ++context;
                if (context.match_literal("an")) {
                    month = 1;
                    break;
                }
                else if (context.match_literal("un")) {
                    month = 6;
                    break;
                }
                else if (context.match_literal("ul")) {
                    month = 7;
                    break;
                }
                else return false;
            }
            case 'f': {
                ++context;
                if (!context.match_literal("eb")) return false;
                month = 2;
                break;
            }
            case 'm': {
                ++context;
                if (context.match_literal("ar")) {
                    month = 3;
                    break;
                }
                else if (context.match_literal("ay")) {
                    month = 5;
                    break;
                }
                else return false;
                break;
            }
            case 'a':
                ++context;
                if (context.match_literal("pr")) {
                    month = 4;
                    break;
                }
                else if (context.match_literal("ug")) {
                    month = 8;
                    break;
                }
                else return false;
                break;
            case 's':
                ++context;
                if (!context.match_literal("ep")) return false;
                month = 9;
                break;
            case 'o':
                ++context;
                if (!context.match_literal("ct")) return false;
                month = 10;
                break;
            case 'n': {
                ++context;
                if (!context.match_literal("ov")) return false;
                month = 11;
                break;
            }
            case 'd': {
                ++context;
                if (!context.match_literal("ec")) return false;
                month = 12;
                break;
            }
            default:
                return false;
            }
            break;
        case 'y':
            context.match_int(year, 1400, 9999);
            break;
        default:
            throw Exception("expect_date: format " + string(1, *f)
                            + " not implemented yet");
        }
    }
    
    try {
        boost::gregorian::date date(year, month, day);
        boost::posix_time::ptime time(date);
        result = Date::fromSecondsSinceEpoch((time - epoch).total_microseconds()/1000000.0);
    } catch (const std::exception & exc) {
        return false;
    }
    token.ignore();
    return true;
}

Date
Date::
expect_date(ParseContext & context, const std::string & format)
{
    int year = -1, month = 1, day = 1;

    for (const char * f = format.c_str();  context && *f;  ++f) {
        if (*f != '%') {
            context.expect_literal(*f);
            continue;
        }
        
        ++f;
        switch (*f) {
        case '%':
            context.expect_literal('%');
            break;
        case 'd':
            day = expectFixedWidthInt(context, 1, 2, 1, 31,
                                      "expected day of month");
            break;
        case 'm':
            month = expectFixedWidthInt(context, 1, 2, 1, 12,
                                        "expected month of year");
            break;
        case 'M':
            switch(tolower(*context)) {
            case '0':
            case '1':
            case '2':
            case '3':
            case '4':
            case '5':
            case '6':
            case '7':
            case '8':
            case '9': {
                month = expectFixedWidthInt(context, 1, 2, 1, 12,
                                            "expected month of year");
                break;
            }
            case 'j': {
                ++context;
                if (context.match_literal("an")) {
                    month = 1;
                    break;
                }
                else if (context.match_literal("un")) {
                    month = 6;
                    break;
                }
                else if (context.match_literal("ul")) {
                    month = 7;
                    break;
                }
                else context.exception("expected month name");
            }
            case 'f': {
                ++context;
                context.expect_literal("eb", "expected Feb");
                month = 2;
                break;
            }
            case 'm': {
                ++context;
                if (context.match_literal("ar")) {
                    month = 3;
                    break;
                }
                else if (context.match_literal("ay")) {
                    month = 5;
                    break;
                }
                else context.exception("expected month name");
                break;
            }
            case 'a':
                ++context;
                if (context.match_literal("pr")) {
                    month = 4;
                    break;
                }
                else if (context.match_literal("ug")) {
                    month = 8;
                    break;
                }
                else context.exception("expected month name");
                break;
            case 's':
                ++context;
                context.expect_literal("ep", "expected Sep");
                month = 9;
                break;
            case 'o':
                ++context;
                context.expect_literal("ct", "expected Oct");
                month = 10;
                break;
            case 'n': {
                ++context;
                context.expect_literal("ov", "expected Nov");
                month = 11;
                break;
            }
            case 'd': {
                ++context;
                context.expect_literal("ec", "expected Dec");
                month = 12;
                break;
            }
            default:
                context.exception("expected month name for %M");
            }
            break;
        case 'y':
            year = expectFixedWidthInt(context, 4, 4, 1400, 2999,
                                       "expected year");
            break;
        default:
            throw Exception("expect_date: format " + string(1, *f)
                            + " not implemented yet");
        }
    }
    
    //cerr << "year = " << year << " month = " << month
    //     << " day = " << day << endl;
    
    try {
        boost::gregorian::date date(year, month, day);
        boost::posix_time::ptime time(date);
        return Date::fromSecondsSinceEpoch((time - epoch).total_microseconds()/1000000.0);
        //result = (time - DateHandler::epoch).total_seconds();
        //cerr << "result = " << result << endl;
    } catch (const std::exception & exc) {
        context.exception("error parsing date: " + string(exc.what()));
        throw Exception("not reached");
    }
    //return result;
}

#if 0
Date
Date::
expect_date(ParseContext & context,
           const std::string & format)
{
    Date result;
    if (!match_date(context, format))
        context.exception("expected date");
    return result;
}
#endif

bool
Date::
match_time(ParseContext & context,
           double & result,
           const std::string & format)
{
    ParseContext::Revert_Token token(context);

    int hour = 0, minute = 0, offset = 0;
    double second = 0;
    bool twelve_hour = false;
    
    for (const char * f = format.c_str();  context && *f;  ++f) {
        if (*f != '%') {
            if (!context.match_literal(*f)) return false;
            continue;
        }
        
        ++f;
        switch (*f) {
        case '%':
            if (!context.match_literal('%')) return false;;
            break;
        case 'h':
            twelve_hour = true;
            if (!context.match_int(hour, 1, 12))
                return false;
            break;
        case 'H':
            twelve_hour = false;
            if (!context.match_int(hour, 0, 24))
                return false;
            if (hour >= 24) return false;
            break;
        case 'M':
            if (!context.match_int(minute, 0, 60))
                return false;
            break;
        case 'S':
            if (!context.match_double(second, 0, 60))
                return false;
            if (second >= 60.0) return false;
            break;
        case 'p':
            twelve_hour = true;
            if (context.match_literal('A')) {
                if (!context.match_literal('M')) return false;
                offset = 0;
            }
            else if (context.match_literal('P')) {
                if (!context.match_literal('M')) return false;
                offset = 12;
            }
            else return false;
            break;
            
        default:
            throw Exception("expect_time: format " + string(1, *f)
                            + " not implemented yet");
        }
    }
    
    if (twelve_hour) {
        if (hour < 1 || hour > 12)
            return false;
        if (hour == 12) hour = 0;
        hour += offset;
    }

    double fractional_sec, full_sec;
    fractional_sec = modf(second, &full_sec);
    
    using namespace boost::posix_time;
    result = (hours(hour) + minutes(minute) + seconds(full_sec)).total_seconds()
        + fractional_sec;
    token.ignore();
    return true;
}

double
Date::
expect_time(ParseContext & context, const std::string & format)
{
    int hour = 0, minute = 0, offset = 0;
    double second = 0;
    bool twelve_hour = false;
    
    for (const char * f = format.c_str();  context && *f;  ++f) {
        if (*f != '%') {
            context.expect_literal(*f);
            continue;
        }
        
        ++f;
        switch (*f) {
        case '%':
            context.expect_literal('%');
            break;
        case 'h':
            twelve_hour = true;
            hour = context.expect_int(1, 12, "expected hours");
            break;
        case 'H':
            twelve_hour = false;
            hour = context.expect_int(0, 24, "expected hours");
            if (hour >= 24)
                context.exception("expected 24-hour hour");
            break;
        case 'M':
            minute = context.expect_int(0, 60, "expected minutes");
            break;
        case 'S':
            second = context.expect_double(0, 60, "expected seconds");
            if (second >= 60.0)
                context.exception("seconds cannot be 60");
            break;
        case 'p':
            twelve_hour = true;
            if (context.match_literal('A')) {
                context.expect_literal('M', "expected AM");
                offset = 0;
            }
            else if (context.match_literal('P')) {
                context.expect_literal('M', "expected AM");
                offset = 12;
            }
            else context.exception("expected AM or PM");
            break;
            
        default:
            throw Exception("expect_time: format " + string(1, *f)
                            + " not implemented yet");
        }
    }
    
    if (twelve_hour) {
        if (hour < 1 || hour > 12)
            context.exception("invalid hour after 12 hours");
        if (hour == 12) hour = 0;
        hour += offset;
    }

    double fractional_sec, full_sec;
    fractional_sec = modf(second, &full_sec);
    
    using namespace boost::posix_time;
    return (hours(hour) + minutes(minute) + seconds(full_sec)).total_seconds()
        + fractional_sec;
}

#if 0
double
Date::
expect_time(ParseContext & context,
           const std::string & format)
{
    double time;
    if (!context.match_time(context, time, format))
        context.exception("expected time");
    return time;
}
#endif



/** 
    DEPRECATED FUNCTION documentation:
    This function takes a string expected to contain a date that matches the
    provided date pattern, followed by a time. The two patterns in the
    string can be separated by whitespace but anything else has to appear in
    the patterns. 
    
    example:

        parse_date_time("2013-05-13/21:00:00", "%y-%m-%d/","%H:%M:%S")

    returns 2013-May-13 21:00:00.
    
    symbols meanings:
        date_format:
            %d      day of month as digit 1-31
            %m      month as digit 1-12
            %M      month as 3-letter abbreviation
            %y      year with century 1400-2999
        time_format:
            %h      hour as digit 1-12
            %H      hour as digit 0-24
            %M      minute as digit 0-60
            %S      second as digit 0-60
            %p      'AM' or 'PM'
**/
Date
Date::
parse_date_time(const std::string & str,
                const std::string & date_format,
                const std::string & time_format)
{
    using namespace boost::posix_time;
    
    if (str == "") return Date::notADate();
    
    Date result;
    try {
        ParseContext context(str,
                                  str.c_str(), str.c_str() + str.length());
        result = expect_date_time(context, date_format, time_format);
        
        context.expect_eof();
    }
    catch (const std::exception & exc) {
        //cerr << "Error parsing date string:\n'" << str << "'" << endl;
        throw;
    }
    
    return result;
}

Date
Date::
expect_date_time(ParseContext & context,
                 const std::string & date_format,
                 const std::string & time_format)
{
    Date date;
    double time = 0.0;
    
    date = expect_date(context, date_format);
    
    if (!context.eof()) {
        ParseContext::Revert_Token token(context);
        context.match_whitespace();
        if (match_time(context, time, time_format))
            token.ignore();
    }
    
    return date.plusSeconds(time);
}

bool
Date::
match_date_time(ParseContext & context,
                Date & result,
                const std::string & date_format,
                const std::string & time_format)
{
    Date date;
    double time = 0.0;
    
    if (!match_date(context, date, date_format)) return false;
    context.match_whitespace();
    match_time(context, time, time_format);
    result = date.plusSeconds(time);

    return true;
}

Date Date::parse(const std::string & date,
                 const std::string & format)
{
    tm time;
    memset(&time, 0, sizeof(time));
    if(strptime(date.c_str(), format.c_str(), &time) == NULL)
        throw MLDB::Exception("strptime error. format='" + format + "', string='" + date + "'");

    //not using fromTm because I don't want it to assume it's local time
    return Date(1900 + time.tm_year, 1 + time.tm_mon, time.tm_mday,
                time.tm_hour, time.tm_min, time.tm_sec);
}

tm
Date::
toTm() const
{
    tm result;
    errno = 0;
    time_t t = toTimeT();
    if (gmtime_r(&t, &result) == 0)
        throw MLDB::Exception("error converting time: t = %lld",
                            (long long)t,
                            strerror(errno));
    return result;
}

Date
Date::
fromTm(const tm & t)
{
    tm t2 = t;
    time_t t3 = mktime(&t2);
    if (t3 == (time_t)-1)
        throw MLDB::Exception("couldn't construct from invalid time");
    return fromTimeT(t3);
}


void Date::addFromString(string str){
    {
        using namespace boost;
        string format = "^[1-9][0-9]*[SMHd]$";
        regex e(format);
        if(!regex_match(str, e)){
            throw MLDB::Exception("String " + str + " did not match format "
                + format);
        }
    }
    char unit = str[str.length() - 1];
    int length;
    {
        stringstream tmp;
        tmp << str.substr(0, -1);
        tmp >> length;
    }
    switch(unit){
        case 'S':
            this->addSeconds(length);
            break;
        case 'M':
            this->addMinutes(length);
            break;
        case 'H':
            this->addHours(length);
            break;
        case 'd':
            this->addDays(length);
            break;
        default:
            throw MLDB::Exception("Should never get here with string: " + str);
            break;
    }
}


/*****************************************************************************/
/* ISO8601PARSER                                                             */
/*****************************************************************************/

Iso8601Parser::
Iso8601Parser(const std::string & dateStr)
    : parser(new ParseContext(dateStr, dateStr.c_str(),
                               dateStr.c_str() + dateStr.size()))
{
}

Iso8601Parser::
~Iso8601Parser()
{
}

Date
Iso8601Parser::
expectDateTime()
{
    Date date;
    if (!matchDateTime(date))
        parser->exception("failed to parse date time");

    return date;
}
   
bool
Iso8601Parser::
matchDateTime(Date & date)
{
    /*
      parse date
      try eof
      or {
        parse 'T' || ' '
        parse time
      }
      try eof
      or {
        parse tz
      }
    */

    Date tempDate;
    if (!matchDate(tempDate))
        return false;

    if (!parser->eof() && (parser->match_literal('T') || parser->match_literal(' '))) {
        Date tempTime;
        if (!matchTime(tempTime))
            return false;
        tempDate.addSeconds(tempTime.secondsSinceEpoch());
    }
    date = tempDate;
    return true;
}
 
Date
Iso8601Parser::
expectDate()
{
    Date date;
    if (!matchDate(date))
        parser->exception("failed to parse date");

    return date;
}

bool
Iso8601Parser::
matchDate(Date &date)
{
    int year(0);
    
    if (!matchYear(year))
        return false;

    parser->match_literal('-');
    if (parser->match_literal('W')) {
        int week(0);
        if (!matchWeekNumber(week))
            return false;
        parser->match_literal('-');
        int day;
        if (parser->eof()) {
            day = 1;
        }
        else {
            if (!matchWeekDay(day))
                return false;
        }
        date = Date::fromIso8601Week(year, week, day);
        return true;
    }
    else {
        int day(1);

        {
            ParseContext::Revert_Token token(*parser);

            if (matchYearDay(day) && (parser->eof() || !isdigit(*(*parser)))) {
                Date tempDate(year, 1, 1);
                tempDate.addDays(day - 1);
                token.ignore();
                date = tempDate;
                return true;
            }
        }

        int month(1);
        if (!parser->eof()) {
            if (!matchMonth(month))
                return false;
            parser->match_literal('-');
            if (!parser->eof() && isdigit(*(*parser))) {
                if (!matchMonthDay(day))
                    return false;
            }
        }
        date = Date(year, month, day);
        return true;
    }
}


Date
Iso8601Parser::
expectTime()
{
    Date date;
    if (!matchTime(date))
        parser->exception("failed to parse time");

    return date;
}

bool
Iso8601Parser::
matchTime(Date & date)
{
    Date tempDate;
    int hours(0);
    if (!matchHours(hours))
        return false;
    tempDate.addHours(hours);
    if (parser->eof()) {
        date = tempDate;
        return true;
    }

    parser->match_literal(':');

    int minutes(0);
    if (!matchMinutes(minutes))
        return false;
    tempDate.addMinutes(minutes);
    if (parser->eof()) {
        date = tempDate;
        return true;
    }

    parser->match_literal(':');
    int seconds(0);
    if (!matchSeconds(seconds))
        return false;
    tempDate.addSeconds(seconds);
    if (parser->eof()) {
        date = tempDate;
        return true;
    }

    if (parser->match_literal('.')) {
        // To parse the fractional seconds properly, we need to convert
        // the non-fractional part to a double, add the fraction, and
        // parse the whole lot as a string.

        string toParse = to_string((int64_t)tempDate.secondsSinceEpoch());
        toParse += '.';
        while (!parser->eof() && isdigit(**parser))
            toParse += *(*parser)++;
        tempDate = Date::fromSecondsSinceEpoch(jsonDecodeStr<double>(toParse));
    }

    if (parser->eof()) {
        date = tempDate;
        return true;
    }

    int tzminutes(0);
    if (matchTimezone(tzminutes)) {
        tempDate.addMinutes(tzminutes);
        date = tempDate;
        return true;
    }
    else if (parser->match_literal('Z')) {
            date = tempDate;
            return true;
        }

    return false;   
}

bool
Iso8601Parser::
matchTimezone(int& tzminutes)
{
    ParseContext::Revert_Token token(*parser);
    if (parser->match_literal('+')) {
        if (matchTimeZoneMinutes(tzminutes)) {
            token.ignore();
            tzminutes = -tzminutes;
            return true;
        }
    }
    else if (parser->match_literal('-')) {
        if (matchTimeZoneMinutes(tzminutes)) {
            token.ignore();
            return true;
        }
    }

    return false;
}

int
Iso8601Parser::
expectTimezone()
{
    int tzminutes(0);
    if (!matchTimezone(tzminutes))
        parser->exception("failed to parse timezone");

    return tzminutes;
}

int
Iso8601Parser::
expectYear()
{
    return expectFixedWidthInt(*parser, 4, 4, 1400, 9999, "bad year");
}

bool
Iso8601Parser::
matchYear(int & result)
{
    return matchFixedWidthInt(*parser, 4, 4, 1400, 9999, result);
}

int
Iso8601Parser::
expectMonth()
{
    return expectFixedWidthInt(*parser, 2, 2, 1, 12, "bad month");
}

bool
Iso8601Parser::
matchMonth(int & result)
{
    return matchFixedWidthInt(*parser, 2, 2, 1, 12, result);
}

int
Iso8601Parser::
expectWeekNumber()
{
    return expectFixedWidthInt(*parser, 2, 2, 1, 53, "bad week number");
}

bool
Iso8601Parser::
matchWeekNumber(int & result)
{
    return matchFixedWidthInt(*parser, 2, 2, 1, 53, result);
}

int
Iso8601Parser::
expectWeekDay()
{
    return expectFixedWidthInt(*parser, 1, 1, 1, 7, "bad week day");
}

bool
Iso8601Parser::
matchWeekDay(int & result)
{
    return matchFixedWidthInt(*parser, 1, 1, 1, 7, result);
}

int
Iso8601Parser::
expectMonthDay()
{
    return expectFixedWidthInt(*parser, 2, 2, 1, 31, "bad month day");
}

bool
Iso8601Parser::
matchMonthDay(int & result)
{
    return matchFixedWidthInt(*parser, 2, 2, 1, 31, result);
}

int
Iso8601Parser::
expectYearDay()
{
    return expectFixedWidthInt(*parser, 3, 3, 1, 366, "bad year day");
}

bool
Iso8601Parser::
matchYearDay(int & result)
{
    return matchFixedWidthInt(*parser, 3, 3, 1, 366, result);
}

int
Iso8601Parser::
expectHours()
{
    return expectFixedWidthInt(*parser, 2, 2, 0, 23, "wrong hour value");
}

bool
Iso8601Parser::
matchHours(int & result)
{
    return matchFixedWidthInt(*parser, 2, 2, 0, 23, result);
}

int
Iso8601Parser::
expectMinutes()
{
    return expectFixedWidthInt(*parser, 2, 2, 0, 59, "bad minute value");
}

bool
Iso8601Parser::
matchMinutes(int & result)
{
    return matchFixedWidthInt(*parser, 2, 2, 0, 59, result);
}

int
Iso8601Parser::
expectSeconds()
{
    return expectFixedWidthInt(*parser, 2, 2, 0, 60, "bad second value");
}

bool
Iso8601Parser::
matchSeconds(int & result)
{
    return matchFixedWidthInt(*parser, 2, 2, 0, 60, result);
}

int
Iso8601Parser::
expectTimeZoneMinutes()
{
    int minutes(0);

    int hours = expectHours();
    parser->match_literal(':');
    matchMinutes(minutes);
    minutes += hours * 60;

    return minutes;
}

bool
Iso8601Parser::
matchTimeZoneMinutes(int & result)
{
    int minutes(0);

    int hours(0);
    if (!matchHours(hours))
        return false;
    bool mustHaveMinutes = parser->match_literal(':');
    bool hasMinutes = matchMinutes(minutes);

    minutes += hours * 60;
    result = minutes;
    return !mustHaveMinutes || hasMinutes;
}

/*****************************************************************************/
/* VALUE DESCRIPTION                                                         */
/*****************************************************************************/

DEFINE_VALUE_DESCRIPTION(Date, DateDescription);

void
DateDescription::
parseJsonTyped(Date * val,
               JsonParsingContext & context) const
{
    if (context.isNumber())
        *val = Date::fromSecondsSinceEpoch(context.expectDouble());
    else if (context.isString()) {
        std::string s = context.expectStringAscii();
        if (s.length() >= 11
            && s[4] == '-'
            && s[7] == '-'
            && (s[s.size() - 1] == 'Z'
                || s[s.size() - 3] == ':')) {
            Date date = Date::parseIso8601DateTime(s);
            if (!date.isADate())
                context.exception("expected date");
            *val = date;
        }
        else *val = Date::parseDefaultUtc(s);
    }
    else context.exception("expected date");
}

void
DateDescription::
printJsonTyped(const Date * val,
                            JsonPrintingContext & context) const
{
    context.writeJson(val->printIso8601());//val->secondsSinceEpoch());
}

bool
DateDescription::
isDefaultTyped(const Date * val) const
{
    return *val == Date();
}

template struct ValueDescriptionI<MLDB::Date, ValueKind::ATOM, DateDescription>;

void
JavaTimestampValueDescription::
parseJsonTyped(Date * val,
               JsonParsingContext & context) const
{
    *val = Date::fromSecondsSinceEpoch(context.expectDouble() * 0.001);
}

void
JavaTimestampValueDescription::
printJsonTyped(const Date * val,
               JsonPrintingContext & context) const
{
    context.writeJson((uint64_t)(val->secondsSinceEpoch() * 1000));
}

void
Iso8601TimestampValueDescription::
parseJsonTyped(Date * val,
               JsonParsingContext & context) const
{
    if (context.isNumber())
        *val = Date::fromSecondsSinceEpoch(context.expectDouble());
    else if (context.isString()){
        Date date = Date::parseIso8601DateTime(context.expectStringAscii());
        if (!date.isADate())
            context.exception("expected date");
        *val = date;
    }
    else context.exception("expected date");
}

void
Iso8601TimestampValueDescription::
printJsonTyped(const Date * val,
               JsonPrintingContext & context) const
{
    //cerr << "Print iso date" << endl;
    context.writeString(val->printIso8601());
}

/*****************************************************************************/
/* TIME UNITS                                                                */
/*****************************************************************************/

TimeUnit ParseTimeUnit(ParseContext& context)
{
    if (context.match_test_icase("MICROSECOND"))  {
        return MICROSECOND;
    }
    else if (context.match_test_icase("MILLISECOND"))  {
        return MILLISECOND;
    }
    else if (context.match_test_icase("SECOND"))  {
        return SECOND;
    }
    else if (context.match_test_icase("MINUTE"))  {
        return MINUTE;
    }
    else if (context.match_test_icase("HOUR"))  {
        return HOUR;
    }
    else if (context.match_test_icase("DAY"))  {
        return DAY;
    }
    else if (context.match_test_icase("DOW"))  {
        return DOW;
    }
    else if (context.match_test_icase("DOY"))  {
        return DOY;
    }
    else if (context.match_test_icase("ISODOW"))  {
        return ISODOW;
    }
     else if (context.match_test_icase("ISODOY"))  {
        return ISODOY;
    }
    else if (context.match_test_icase("WEEK"))  {
        return WEEK;
    }
    else if (context.match_test_icase("ISOWEEK"))  {
        return ISOWEEK;
    }
    else if (context.match_test_icase("MONTH"))  {
        return MONTH;
    }
    else if (context.match_test_icase("QUARTER"))  {
        return QUARTER;
    }
    else if (context.match_test_icase("YEAR"))  {
        return YEAR;
    }
    else if (context.match_test_icase("ISOYEAR"))  {
        return ISOYEAR;
    }

    return TIMEUNIT_INVALID;
}

TimeUnit ParseTimeUnit(std::string& sinput)
{
    ParseContext context(sinput, sinput.c_str(), sinput.c_str() + sinput.size());
    return ParseTimeUnit(context);
}

} // namespace MLDB
