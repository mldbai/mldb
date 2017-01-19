/* date.h                                                          -*- C++ -*-
   Jeremy Barnes, 18 July 2010
   Copyright (c) 2010 mldb.ai inc.  All rights reserved.

   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

   Basic class that holds and manipulates a date.  Not designed for ultimate
   accuracy, but shouldn't be too bad.
*/

#pragma once

#include <chrono>
#include <string>
#include <cmath>
#include "value_description_fwd.h"

namespace Json {

class Value;

} // namespace Json


namespace MLDB {
namespace JS {
struct JSValue;
} // namespace JS
struct ParseContext;

/*****************************************************************************/
/* TIME UNITS                                                                */
/*****************************************************************************/

enum TimeUnit
{    
    MICROSECOND,
    MILLISECOND,
    SECOND,
    MINUTE,
    HOUR,
    DAY,
    DOW,
    DOY,
    ISODOW,
    ISODOY,
    WEEK,
    ISOWEEK,
    MONTH,
    QUARTER,
    YEAR,
    ISOYEAR,
    TIMEUNIT_INVALID
};

TimeUnit ParseTimeUnit(ParseContext& context);
TimeUnit ParseTimeUnit(std::string& sinput);

/*****************************************************************************/
/* DATE                                                                      */
/*****************************************************************************/

struct Date {

    Date()
        : secondsSinceEpoch_(0.0)
    {
    }

    Date(int year, int month, int day,
         int hour = 0, int minute = 0, int second = 0,
         double fraction = 0.0);
    explicit Date(JS::JSValue & value);
    explicit Date(const Json::Value & value);

    static Date fromSecondsSinceEpoch(double numSeconds)
    {
        Date result;
        result.secondsSinceEpoch_ = numSeconds;
        return result;
    }

    static Date fromTimespec(const struct timespec & ts)
    {
        Date result;
        result.secondsSinceEpoch_ = (double(ts.tv_sec)
                                     + double(ts.tv_nsec) / 1000000000);
        return result;
    }

    static Date fromTimeval(const struct timeval & ts)
    {
        Date result;
        result.secondsSinceEpoch_ = (double(ts.tv_sec)
                                     + double(ts.tv_usec) / 1000000);
        return result;
    }

    static Date fromIso8601Week(int year, int week, int day = 1);

    static Date parseSecondsSinceEpoch(const std::string & date);

    static Date parseDefaultUtc(const std::string & date);
    static Date parseIso8601DateTime(const std::string & date);

    // Deprecated
    static Date parseIso8601(const std::string & date);

    static Date notADate();
    static Date positiveInfinity();
    static Date negativeInfinity();
    static Date now();

    bool isADate() const;

    double secondsSinceEpoch() const
    {
        return secondsSinceEpoch_;
    }

    std::string print(int seconds_digits = -1) const;
    std::string print(const std::string & format) const;
    std::string printIso8601(int seconds_digits = -1) const;
    std::string printRfc2616() const;
    std::string printClassic() const;

    bool operator == (const Date & other) const
    {
        return secondsSinceEpoch_ == other.secondsSinceEpoch_;
    }

    bool operator != (const Date & other) const
    {
        return ! operator == (other);
    }

    bool operator <  (const Date & other) const
    {
        return secondsSinceEpoch_ < other.secondsSinceEpoch_;
    }

    bool operator <= (const Date & other) const
    {
        return secondsSinceEpoch_ <= other.secondsSinceEpoch_;
    }

    bool operator >  (const Date & other) const
    {
        return secondsSinceEpoch_ > other.secondsSinceEpoch_;
    }

    bool operator >= (const Date & other) const
    {
        return secondsSinceEpoch_ >= other.secondsSinceEpoch_;
    }

    double operator + (const Date & other) const
    {
        return secondsSinceEpoch_ + other.secondsSinceEpoch_;
    }

    double operator - (const Date & other) const
    {
        return secondsSinceEpoch_ - other.secondsSinceEpoch_;
    }
    
    Date & setMin(Date other)
    {
        secondsSinceEpoch_ = std::min(secondsSinceEpoch_,
                                      other.secondsSinceEpoch_);
        return *this;
    }

    Date & setMax(Date other)
    {
        secondsSinceEpoch_ = std::max(secondsSinceEpoch_,
                                      other.secondsSinceEpoch_);
        return *this;
    }

    /** Quantize to the given fraction of a second.  For example,
        quantize(0.1) leaves only tenths of a second, whereas quantize(1)
        quantizes to the nearest second. */
    Date quantized(double fraction) const;
    Date & quantize(double fraction);

    Date & addSeconds(double interval)
    {
        secondsSinceEpoch_ += interval;
        return *this;
    }

    Date & addMinutes(double interval)
    {
        secondsSinceEpoch_ += interval * 60.0;
        return *this;
    }

    Date & addHours(double interval)
    {
        secondsSinceEpoch_ += interval * 3600.0;
        return *this;
    }

    Date & addDays(double interval)
    {
        secondsSinceEpoch_ += interval * 3600.0 * 24.0;
        return *this;
    }

    Date & addWeeks(double interval)
    {
        addDays(interval * 7.0);
        return *this;
    }

    Date plusSeconds(double interval) const
    {
        Date result = *this;
        result.addSeconds(interval);
        return result;
    }

    Date plusMinutes(double interval) const
    {
        Date result = *this;
        result.addSeconds(interval * 60.0);
        return result;
    }

    Date plusHours(double interval) const
    {
        Date result = *this;
        result.addSeconds(interval * 3600.0);
        return result;
    }

    Date plusDays(double interval) const
    {
        Date result = *this;
        result.addSeconds(interval * 3600.0 * 24.0);
        return result;
    }

    Date plusWeeks(double interval) const
    {
        return plusDays(interval * 7.0);
    }

    Date plusMonthDaySecond(int64_t months, int64_t days, float seconds) const;
    Date minusMonthDaySecond(int64_t months, int64_t days, float seconds) const;

    /** Return the (integral) number of days and the number of seconds between
        this timestamp and another timestamp.
    */
    std::tuple<int64_t, float> getDaySecondInterval(const Date& rDate) const;

    double secondsUntil(const Date & other) const
    {
        return other.secondsSinceEpoch_ - secondsSinceEpoch_;
    }

    double minutesUntil(const Date & other) const
    {
        static const double factor = 1.0 / 60.0;
        return secondsUntil(other) * factor;
    }

    double hoursUntil(const Date & other) const
    {
        static const double factor = 1.0 / 3600.0;
        return secondsUntil(other) * factor;
    }

    double daysUntil(const Date & other) const
    {
        static const double factor = 1.0 / 24.0 / 3600.0;
        return secondsUntil(other) * factor;
    }

    double secondsSince(const Date & other) const
    {
        return -secondsUntil(other);
    }

    double minutesSince(const Date & other) const
    {
        return -minutesUntil(other);
    }

    double hoursSince(const Date & other) const
    {
        return -hoursUntil(other);
    }

    double daysSince(const Date & other) const
    {
        return -daysUntil(other);
    }

    bool sameDay(const Date & other) const
    {
        return dayStart() == other.dayStart();
    }

    Date weekStart() const
    {
        int delta = weekday();
        return plusDays(-delta).dayStart();
    }
    Date iso8601WeekStart() const
    {
        int nbr = iso8601Weekday();
        return (nbr == 1
                ? dayStart()
                : plusDays(1-nbr).dayStart());
    }
    Date dayStart() const
    {
        static const double secPerDay = 24.0 * 3600.0;
        double day = secondsSinceEpoch_ / secPerDay;
        double startOfDay = floor(day);
        return fromSecondsSinceEpoch(startOfDay * secPerDay);
    }
    Date hourStart() const
    {
        static const double secPerHour = 3600.0;
        double hour = secondsSinceEpoch_ / secPerHour;
        double startOfHour = floor(hour);
        return fromSecondsSinceEpoch(startOfHour * secPerHour);
    }

    int hour() const;
    int minute() const;
    int second() const;
    int millisecond() const;
    int microsecond() const;
    int weekday() const;
    int iso8601Weekday() const;
    int dayOfMonth() const;
    int dayOfYear() const;
    int iso8601WeekOfYear() const;
    int week() const;
    int monthOfYear() const;
    int quarter() const;
    int year() const;
    int iso8601Year() const;
    int iso8601DayofYear() const;

    int hourOfWeek() const;

    double fractionalSeconds() const
    {
        double whole_seconds;
        return modf(secondsSinceEpoch_ >= 0
                    ? secondsSinceEpoch_ : -secondsSinceEpoch_,
                    &whole_seconds);
    }

    long long wholeSecondsSinceEpoch() const
    {
        double whole_seconds;
        modf(secondsSinceEpoch_, &whole_seconds);
        return whole_seconds;
    }

    int get(TimeUnit unit) const;

    Date trunc(TimeUnit unit) const;

    std::string printMonth() const;
    std::string printWeekday() const;
    std::string printYearAndMonth() const;

    //static const boost::posix_time::ptime epoch;

    static Date expect_date(ParseContext & context,
                            const std::string & format);
    static bool match_date(ParseContext & context, Date & date,
                           const std::string & format);
    
    static double expect_time(ParseContext & context,
                              const std::string & format);
    static bool match_time(ParseContext & context,
                           double & time,
                           const std::string & format);

    static Date expect_date_time(ParseContext & context,
                                 const std::string & date_format,
                                 const std::string & time_format);

    static bool match_date_time(ParseContext & context,
                                Date & result,
                                const std::string & date_format,
                                const std::string & time_format);

    // DO NOT USE THIS FUNCTION. IT IS DEPRECATED.
    static Date parse_date_time(const std::string & date_time,
                                const std::string & date_format,
                                const std::string & time_format);

    // parse using strptime function. more compatible with the `print` format
    static Date parse(const std::string & date,
                      const std::string & format);

    size_t hash() const
    {
        return std::hash<double>() (secondsSinceEpoch_);
    }

    /** Convert to a std::chrono::time_point<std::chrono::system_clock>
        for the standard timing functions.
    */
    std::chrono::time_point<std::chrono::system_clock>
    toStd() const
    {
        return std::chrono::system_clock::from_time_t(toTimeT())
            + std::chrono::microseconds(static_cast<long>(1000000 * fractionalSeconds()));
    }

    /** Convert to a time_t value (seconds).  Rounds fractional seconds
        down.
    */
    time_t toTimeT() const
    {
        return secondsSinceEpoch();
    }

    /** Construct from a time_t value. */
    static Date fromTimeT(const time_t & time)
    {
        return Date::fromSecondsSinceEpoch(time);
    }

    /** Convert to a tm structure. */
    tm toTm() const;

    /** Construct from a tm structure. */
    static Date fromTm(const tm & t);

    /** expects a ^[1-9][0-9]*[SMHd]$ string */
    void addFromString(std::string);

private:
    double secondsSinceEpoch_;
    Date(double);
};

std::ostream & operator << (std::ostream & stream, const Date & date);
std::istream & operator >> (std::istream & stream, Date & date);

namespace JS {

void to_js(JSValue & jsval, Date value);
Date from_js(const JSValue & val, Date *);

inline Date
from_js_ref(const JSValue & val, Date *)
{
    return from_js(val, (Date *)0);
}


} // namespace JS


/*****************************************************************************/
/* ISO8601PARSER                                                             */
/*****************************************************************************/

 struct Iso8601Parser {

    static Date parseDateTimeString(const std::string & dateTimeStr)
    {
        Iso8601Parser parser(dateTimeStr);
        return parser.expectDateTime();
    }

    static Date parseTimeString(const std::string & timeStr)
    {
        Iso8601Parser parser(timeStr);
        return parser.expectTime();
    }

     Iso8601Parser(const std::string & dateStr);
     ~Iso8601Parser();
     
    Date expectDateTime();
    bool matchDateTime(Date & date);
    Date expectDate();
    bool matchDate(Date & date);
    Date expectTime();
    bool matchTime(Date & date);
    bool matchTimezone(int& tzminutes);
    int  expectTimezone();

private:
    int expectYear();
    bool matchYear(int & year);
    int expectMonth();
    bool matchMonth(int & month);
    int expectWeekNumber();
    bool matchWeekNumber(int & weekNumber);
    int expectWeekDay();
    bool matchWeekDay(int & weekDay);
    int expectMonthDay();
    bool matchMonthDay(int & monthDay);
    int expectYearDay();
    bool matchYearDay(int & result);
    int expectHours();
    bool matchHours(int & result);
    int expectMinutes();
    bool matchMinutes(int & result);
    int expectSeconds();
    bool matchSeconds(int & result);
    int expectTimeZoneMinutes();
    bool matchTimeZoneMinutes(int & result);

     std::unique_ptr<ParseContext> parser;
 };

PREDECLARE_VALUE_DESCRIPTION(Date);

ValueDescriptionT<Date> * getStandardDateDescription();
ValueDescriptionT<Date> * getJavaTimestampDescription();
ValueDescriptionT<Date> * getIso8601TimestampDescription();

} // namespace MLDB

namespace std {

template<>
struct hash<MLDB::Date> {
    size_t operator () (const MLDB::Date & date) const
    {
        return date.hash();
    }
};

} // namespace std

