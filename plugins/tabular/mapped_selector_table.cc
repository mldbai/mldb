/* mapped_selector_table.cc                                             -*- C++ -*-
   Jeremy Barnes, 6 November 2015
   Copyright (c) 2015 mldb.ai inc.  All rights reserved.

   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
*/

#include "mapped_selector_table.h"
#include "utils/bits.h"
#include "mldb/types/structure_description.h"

using namespace std;

namespace MLDB {

void SelectorTableStats::init(std::span<const uint8_t> vals)
{
    IntTableStats<uint8_t>::init(vals);

    // We also need the maximum count
    counts.clear();  counts.resize(maxValue + 1);
    for (auto & v: vals)
        maxCount = std::max(maxCount, ++counts[v]);
    countStats.init(counts);
}

std::pair<uint8_t, uint32_t> MappedSelectorTable::at(uint32_t index) const
{
    auto val = selector_.at(index);
    ExcAssert(val < 256);

    auto countLocation = index >> countEveryNBits_;
    size_t partialCount = 0;

    // First set of counts are implicitly all zero, so we don't store them
    if (countLocation > 0) {
        if (skipSelector_) {
            if (val == skippedSelector_) {
                // Sum of counts at a point must be the index of that point, so we start from there
                // and subtract the other counts; what's left over must be our count for this entry
                partialCount = countLocation << countEveryNBits_;
                auto countIndex = maxSelector_ * (countLocation - 1);
                for (size_t i = 0;  i < maxSelector_;  ++i) {
                    partialCount -= countEntries_.at(countIndex + i);
                }
            }
            else {
                auto countIndex = maxSelector_ * (countLocation - 1) + val - (val > skippedSelector_);
                partialCount = countEntries_.at(countIndex);
            }
        }
        else {
            auto countIndex = (maxSelector_ + 1) * (countLocation - 1) + val;
            partialCount = countEntries_.at(countIndex);
        }

        //cerr << "index " << index << " val " << val << " countLocation " << countLocation << " countEveryNBits_ " << countEveryNBits_
        //     << " contIndex " << countIndex << " maxSelector " << maxSelector_ << " partialCount " << partialCount << endl;
    }

    // Count from the partial count up to the value
    auto count = partialCount + selector_.countValues(countLocation << countEveryNBits_, index, val);
    return { val, count };
}

size_t MappedSelectorTable::size() const
{
    return selector_.size();
}

// For testing; not exported
bool allowSkipSelectors = true;

namespace {

struct SelectorTableBuilder {
    SelectorTableBuilder(const SelectorTableStats & stats)
        : stats(stats)
    {
        if (stats.size == 0) {
            return;
        }

        if (allowSkipSelectors /* TODO: better condition */) {
            skipEntry = stats.countStats.maxElement;
        }

        bitsPerEntry = 8.0 * raw_mapped_indirect_bytes(stats.size, stats.maxValue) / stats.size;
        bitsPerCountEntry = 8.0 * raw_mapped_indirect_bytes(stats.size * (stats.maxValue + 1 - (skipEntry != -1)), stats.maxCount) / stats.size;

        // How much more memory would we need to store all partial counts vs just the entries?
        ratio = bitsPerEntry == 0 ? 0 : bitsPerCountEntry / bitsPerEntry;

        countEveryNEntriesBits = bits(std::max<uint32_t>(ratio, 64));
        countEveryNEntries = 1 << countEveryNEntriesBits;

        // Make a pretend count table to identify runs
        numCountEntriesWritten = stats.size / countEveryNEntries;
        numValuesPerCountEntry = stats.maxValue + 1 - (skipEntry != -1);

        std::vector<uint32_t> countEntries;
        countEntries.reserve(numCountEntriesWritten * numValuesPerCountEntry);

        // Push fake count values to get a sense for what the table looks like
        for (size_t i = 0;  i < stats.size;  ++i) {
            if ((i + 1) % (countEveryNEntries) == 0) {
                for (size_t j = 0;  j < stats.counts.size();  ++j) {
                    if ((int)j != skipEntry)
                        countEntries.push_back(stats.counts[j] / i);
                }
            }
        }

        ExcAssert(countEntries.size() == numCountEntriesWritten * numValuesPerCountEntry);
        countStats.init(countEntries);
    }
    
    SelectorTableStats stats;

    int skipEntry = -1;
    double bitsPerEntry = 0.0;
    double bitsPerCountEntry = 0.0;
    double ratio = 0.0;
    size_t countEveryNEntriesBits = 0;
    size_t countEveryNEntries = 0;
    size_t numUsedSelectors = 0;
    size_t numCountEntriesWritten = 0;
    size_t numValuesPerCountEntry = 0;
    IntTableStats<uint32_t> countStats;  // approximate... 

    size_t bytesRequired() const
    {
        if (stats.size == 0)
            return 0;

        //cerr << "*** expected " << numCountEntriesWritten * numValuesPerCountEntry << " count entries: stats.size " << stats.size
        //    << " maxSelector " << (int)stats.countStats.maxElement << " maxSelectorCount " << stats.countStats.maxValue << " every " << countEveryNEntries << endl;

        size_t selectorBytes = raw_mapped_indirect_bytes(stats);
        size_t countBytes = raw_mapped_indirect_bytes(countStats);

        //cerr << "selector table bytes: " << selectorBytes << endl;
        //cerr << "count bytes: " << countBytes << endl;

        return selectorBytes + countBytes;
    }

    void freeze(MappingContext & context, MappedSelectorTable & output, const std::span<const uint8_t> & input)
    {
        std::vector<uint32_t> countEntries;
        std::vector<uint32_t> accum(stats.maxValue + 1);

        for (size_t i = 0;  i < input.size();  ++i) {
            accum[input[i]] += 1;
            if ((i + 1) % (countEveryNEntries) == 0) {
                for (size_t j = 0;  j < accum.size();  ++j) {
                    if ((int)j != skipEntry)
                        countEntries.push_back(accum[j]);
                }
            }
        }

        ExcAssert(input.size() == stats.size);

        output.countEveryNBits_ = countEveryNEntriesBits;
        output.maxSelector_ = stats.maxValue;

        if (skipEntry == -1) {
            output.skippedSelector_ = 255;  // to avoid uninitialized bits
            output.skipSelector_ = false;
        }
        else {
            output.skippedSelector_ = skipEntry;
            output.skipSelector_ = true;
        }
        ExcAssert(output.countEveryNBits_ == countEveryNEntriesBits);

        freeze_field(context, "selector", output.selector_, input);
        freeze_field(context, "countEntries", output.countEntries_, countEntries);
    }
};

} // file scope

void freeze(MappingContext & context, MappedSelectorTable & output, const std::span<const uint8_t> & input)
{
    SelectorTableBuilder builder(input);
    builder.freeze(context, output, input);
}

size_t mapped_selector_table_bytes_required(const SelectorTableStats & stats)
{
    SelectorTableBuilder builder(stats);
    return builder.bytesRequired();
}

DEFINE_STRUCTURE_DESCRIPTION_INLINE(MappedSelectorTable)
{
    addBitField("maxSelector",     &MappedSelectorTable::flags_, 0, 8, "Highest value of the selector");
    addBitField("skippedSelector", &MappedSelectorTable::flags_, 8, 8, "Which selector is skipped?  Only meaningful is skipSelector_ is true");
    addBitField("countEveryNBits", &MappedSelectorTable::flags_, 16, 5, "We store a cumulative count every 2^n of selector totals");
    addBitField("skipSelector",    &MappedSelectorTable::flags_, 21, 1, "We store a cumulative count every 2^n of selector totals");

    addField("selector", &MappedSelectorTable::selector_, "For each entry, which selector is chosen");
    addField("countEntries", &MappedSelectorTable::countEntries_, "Sparse periodic table of cumulative selector counts to limit countValues range");
}

} // namespace MLDB
