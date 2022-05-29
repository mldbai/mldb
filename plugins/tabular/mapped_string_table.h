/* mapped_string_table.h                                          -*- C++ -*-
   Jeremy Barnes, 6 November 2015
   Copyright (c) 2015 mldb.ai inc.  All rights reserved.

   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
*/

#pragma once

#include "mmap.h"
#include "mapped_int_table.h"
#include "string_table.h"

namespace MLDB {

// A string table is a more efficient way of holding a set of strings.  It serializes the concatenated
// set of strings in a single block of text, and a table of offsets holding the beginning and end
// position of each string in the table.  This table of offsets is often very compressible.
//
// For example, if we wanted to add "Dog", "Cat" and "Horse" to a string table, we would end up with:
//
// Text: "DogCatHorse"
// Offsets: [0, 3, 6, 11]
struct MappedStringTable {

    MappedIntTable<uint32_t> offsets;  // Offset table of beginning of each string (+1 at the end of the end of the last)
    MappedPtr<char> mem; // 

    // Get a string at the given index of the string table.
    std::string_view get(int i) const
    {
        int start = offsets.at(i);
        int end = offsets.at(i + 1);

        int dist = end - start;

        return std::string_view(mem.get() + start, dist);
    }

    size_t size() const
    {
        return offsets.size() - 1;
    }

    using Iterator = StringTableIterator<MappedStringTable, std::string_view>;

    Iterator begin() const
    {
        return {this, 0};
    }

    Iterator end() const
    {
        return {this, size()};
    }

    size_t memUsageIndirect(const MemUsageOptions & /*opt*/) const
    {
        return 0;
    }
};

// Freezing a string table involves freezing both the offsets and the block of text.
inline void freeze(MappingContext & context, MappedStringTable & output, const StringTable & input)
{
    freeze_field(context, "offsets", output.offsets, input.offsets);
    freeze_field(context, "mem", output.mem, input.mem);
}

struct MappedCharacterRangeTableBase {
    MappedIntTable<uint32_t> codeRanges;
};

struct MappedCharacterRangeTable: public CharacterRangeTableImpl<MappedCharacterRangeTableBase> {};

inline void freeze(MappingContext & context, MappedCharacterRangeTable & output, const CharacterRangeTable & input)
{
    freeze(context, output.codeRanges, input.codeRanges);
}

struct MappedEntropyEncoderDecoderBase {
    MappedCharacterRangeTable characterCodes;
    MappedCharacterRangeTable capitalizationCodes;
};

struct MappedEntropyEncoderDecoder: public EntropyEncoderDecoderImpl<MappedEntropyEncoderDecoderBase> {};

inline void freeze(MappingContext & context, MappedEntropyEncoderDecoder & output, const EntropyEncoderDecoder & input)
{
    freeze_field(context, "characterCodes", output.characterCodes, input.characterCodes);
    freeze_field(context, "capitalizationCodes", output.capitalizationCodes, input.capitalizationCodes);
}

struct MappedSuffixDecoderBase {
    MappedStringTable charToPrefix;
};

struct MappedSuffixDecoder: public SuffixDecoderImpl<MappedSuffixDecoderBase> {};

inline void freeze(MappingContext & context, MappedSuffixDecoder & output, const SuffixDecoder & input)
{
    freeze_field(context, "charToPrefix", output.charToPrefix, input.charToPrefix);
}

struct MappedOptimizedStringTableBase {
    MappedStringTable encoded;
    CapStyleDecoder capDecoder;
    MappedSuffixDecoder suffixDecoder;
    MappedEntropyEncoderDecoder entropyDecoder;
};

struct MappedOptimizedStringTable: public OptimizedStringTableImpl<MappedOptimizedStringTableBase> {};

inline void freeze(MappingContext & context, MappedOptimizedStringTable & output, const OptimizedStringTable & input)
{
    freeze_field(context, "encoded", output.encoded, input.encoded);
    freeze_field(context, "capDecoder", output.capDecoder, input.capDecoder);
    freeze_field(context, "suffixDecoder", output.suffixDecoder, input.suffixDecoder);
    freeze_field(context, "entropyDecoder", output.entropyDecoder, input.entropyDecoder);
}

} // namespace MLDB
