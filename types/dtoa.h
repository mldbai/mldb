// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/** dtoa.h

    Print a floating point number at the precision necessary to preserve its
    binary representation, no more.
    
    Derived from dtoa.c.  See copyright and authorship information in that file.
*/

#pragma once

#include <string>

namespace MLDB {

// Convert a float or double to a string. This uses the smallest number of
// characters possible that will result in the right number being read back
// in, EXCEPT for when biasAgainstScientific is non-zero. In that case, it
// will write out up to biasAgainstScientific zeros at the end of integers
// to make them more readable.
//
// Examples
//
// 1e0 -> "1" (always as it's shorter)
// 1e1 -> "10" (always as it's shorter)
// 1e2 -> "100" (always as it's just as short and more readable)
// 1e3 -> "1000" (if biasAgainstScientific >= 1) or "1e3" (if biasAgainstScientific < 1)
// 1e4 -> "10000" (if biasAgainstScientific >= 2) or "1e4" (if biasAgainstScientific < 2)
// 1e5 -> "100000" (if biasAgainstScientific >= 3) or "1e5" (if biasAgainstScientific < 3)
// etc
// 
// The default value of 6 allows numbers up to one million to be written out as integers.

std::string dtoa(double floatVal, int biasAgainstScientific = 5);
std::string ftoa(float floatVal, int biasAgainstScientific = 5);

} // namespace MLDB
