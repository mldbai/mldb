// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* date.h                                                          -*- C++ -*-
   Wolfgang Sourdeau, 10 December 2012
   Copyright (c) 2012 mldb.ai inc.  All rights reserved.

   Timezone-aware implementation of the Date class.
*/

#pragma once

#include <string>

namespace MLDB {

/*****************************************************************************/
/* LOCALDATE                                                                 */
/*****************************************************************************/

struct LocalDate {
    LocalDate(double secondsSinceEpoch = 0, const std::string & tzName = "UTC");

    double secondsSinceEpoch() const;
    const std::string timezone() const;
    int tzOffset() const;

    int hour() const;
    int dayOfMonth() const;
    int dayOfWeek() const;
    int year() const;

private:
    double secondsSinceEpoch_;
    std::string tzName_;
    int tzOffset_; /* updated when either secondsSinceEpoch_ or tzName_
                    * changes */

    void fillTM(struct tm & time) const;
    std::string findTimezoneSpec() const;


    void recomputeTZOffset();
};

} // namespace MLDB
