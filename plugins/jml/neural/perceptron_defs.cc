// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* perceptron_defs.cc
   Jeremy Barnes, 25 May 2009
   Copyright (c) 2009 Jeremy Barnes.  All rights reserved.

   Definitions for the perceptron.
*/

#include "perceptron_defs.h"
#include <iostream>
#include "mldb/utils/string_functions.h"
#include "mldb/jml/db/persistent.h"

using namespace std;
using namespace MLDB;

namespace ML {

std::string print(Transfer_Function_Type act)
{
    switch (act) {
    case TF_LOGSIG:   return "LOGSIG";
    case TF_TANH:     return "TANH";
    case TF_TANHS:    return "TANHS";
    case TF_IDENTITY: return "IDENTITY";
    case TF_SOFTMAX: return "SOFTMAX";
    case TF_NONSTANDARD: return "NONSTANDARD";

    default: return format("Transfer_Function_Type(%d)", act);
    }
}

std::ostream & operator << (std::ostream & stream, Transfer_Function_Type act)
{
    return stream << print(act);
}

BYTE_PERSISTENT_ENUM_IMPL(Transfer_Function_Type);

std::ostream & operator << (std::ostream & stream, Sampling smp)
{
    switch (smp) {
    case SAMP_DETERMINISTIC:     return stream << "DETERMINISTIC";
    case SAMP_BINARY_STOCHASTIC: return stream << "STOCHASTIC BINARY";
    case SAMP_REAL_STOCHASTIC:   return stream << "STOCHASTIC REAL";
    default: return stream << format("Sampling(%d)", smp);
    }
}

BYTE_PERSISTENT_ENUM_IMPL(Sampling);


const Enum_Opt<MLDB::Transfer_Function_Type>
Enum_Info<MLDB::Transfer_Function_Type>::
OPT[Enum_Info<MLDB::Transfer_Function_Type>::NUM] = {
    { "logsig",      MLDB::TF_LOGSIG   },
    { "tanh",        MLDB::TF_TANH     },
    { "tanhs",       MLDB::TF_TANHS    },
    { "identity",    MLDB::TF_IDENTITY },
    { "softmax",     MLDB::TF_SOFTMAX },
    { "nonstandard", MLDB::TF_NONSTANDARD }
};

const char * Enum_Info<MLDB::Transfer_Function_Type>::NAME
    = "Transfer_Function_Type";

const Enum_Opt<MLDB::Sampling>
Enum_Info<MLDB::Sampling>::OPT[Enum_Info<MLDB::Sampling>::NUM] = {
    { "deterministic", MLDB::SAMP_DETERMINISTIC },
    { "stochastic_bin", MLDB::SAMP_BINARY_STOCHASTIC },
    { "stochastic_real", MLDB::SAMP_REAL_STOCHASTIC }
};

const char * Enum_Info<MLDB::Sampling>::NAME = "Sampling";

} // namespace ML
