/* run_length_int_table.cc                                              -*- C++ -*-
   Jeremy Barnes, 6 November 2015
   Copyright (c) 2015 mldb.ai inc.  All rights reserved.

   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
*/

#include "run_length_int_table.h"
#include "run_length_int_table_impl.h"
#include "factored_int_table.h"

using namespace std;

namespace MLDB {

void freeze(MappingContext & context, MappedRunLengthIntTable & output, const RunLengthIntTable & input)
{
    freeze_field(context, "runs", output.runs, input.runs);
    freeze_field(context, "values", output.values, input.values);
}

void freeze(MappingContext & context, MappedRunLengthIntTable & output, const std::span<const uint32_t> & input)
{
    bool validate = context.getOption(VALIDATE);
    RunLengthIntTable temp = runLengthEncode(input, validate);
    freeze(context, output, temp);
}

size_t run_length_indirect_bytes(const IntTableStats<uint32_t> & stats)
{
    IntTableStats<uint32_t> runStats;
    runStats.size = stats.numRuns + 1;
    runStats.maxValue = stats.size;

    IntTableStats<uint32_t> valueStats;
    valueStats.size = stats.numRuns;
    valueStats.maxValue = stats.maxValue;

    size_t run_bytes = decltype(declval<MappedRunLengthIntTable>().runs)::indirectBytesRequired(runStats);
    size_t value_bytes = decltype(declval<MappedRunLengthIntTable>().values)::indirectBytesRequired(valueStats);

    return run_bytes + value_bytes;

    //cerr << "run length bytes: runs " << stats.numRuns << " max run length " << stats.maxRunLength << " maxValue " << stats.maxValue << endl;
    //cerr << "run table: [" << stats.numRuns + 1 << "," << stats.maxRunLength << "] = " << bit_compressed_indirect_bytes(stats.numRuns + 1, stats.maxRunLength) << endl;
    //cerr << "value table: [" << stats.numRuns << "," << stats.maxValue << "] = " << bit_compressed_indirect_bytes(stats.numRuns, stats.maxValue) << endl;
    return bit_compressed_indirect_bytes(stats.numRuns + 1, stats.size)  // array holding the run beginnings
         + bit_compressed_indirect_bytes(stats.numRuns, stats.maxValue); // array holding the values    
}

RunLengthIntTable runLengthEncode(const std::span<const uint32_t> & values, bool validate)
{
    RunLengthIntTable result;

    for (size_t i = 0;  i < values.size();  ++i) {
        if (i == 0 || values[i] != values[i - 1]) {
            result.runs.push_back(i);
            result.values.push_back(values[i]);
        }
    }
    result.runs.push_back(values.size());

    for (size_t i = 0;  i < values.size() && validate;  ++i) {
        if (result.at(i) != values[i]) {
            using namespace std;
            cerr << "error on element " << i << ": expected " << values[i] << " got " << result.at(i) << endl;
            cerr << "runs: ";
            for (auto r: result.runs) cerr << " " << r;
            cerr << endl << "values: ";
            for (auto r: result.values) cerr << " " << r;
            cerr << endl;
            ExcAssert(false);
        }
    }

    return result;
}

// Ensure instantiation of methods marked inline
auto runLengthAtAddress = &MappedRunLengthIntTable::at;
auto runLengthcountValuesAddress = &MappedRunLengthIntTable::countValues;
auto runLengthindirectBytesRequiredAddress = &MappedRunLengthIntTable::indirectBytesRequired;

} // namespace MLDB
