/* factored_int_table.cc                                              -*- C++ -*-
   Jeremy Barnes, 6 November 2015
   Copyright (c) 2015 mldb.ai inc.  All rights reserved.

   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
*/

#include "factored_int_table.h"
#include "factored_int_table_impl.h"
#include "mldb/types/structure_description.h"
#include "mmap_impl.h"

using namespace std;

namespace MLDB {

// Option to control this when serializing
MappingOption<bool> ALLOW_64_BIT_FACTORS("ALLOW_64_BIT_FACTORS", allow64BitFactors);
//MappingOption<size_t> FACTORED_INTERNAL_WORDS("FACTORED_INTERNAL_WORDS", 1);

void freeze_factored_table(MappingContext & context, MappedFactoredIntTable & output, const FactoredIntTable & input,
                           size_t internalWords)
{
    output.size_ = input.size_;
    size_t numWords = input.data_.size();

    //auto internalWords = context.getOption(FACTORED_INTERNAL_WORDS);

    output.factor_ = input.factor_ < 0 ? 0 : input.factor_;
    output.numFactors_= input.numFactors_;
    output.is64_ = input.is64_;

    //cerr << "input.size() " << input.size() << " numWords " << numWords << " internalWords " << internalWords << endl;
    if (input.size() < MappedFactoredIntTable::INTERNAL_OFFSET && numWords <= internalWords + 1) {
        output.size_ = input.size();
        std::copy(input.data_.begin(), input.data_.end(), output.internal_);
    }
    else {
        output.size_ = input.size() + MappedFactoredIntTable::INTERNAL_OFFSET;
        output.data_.set(context.array_field("data_", input.data_.data(), input.data_.size()));
    }

    // Precompute constants to avoid divisions
    //output.M_dFactor = fastmod::computeM_u64(input.factor_);
    //output.M_dnFactors = fastmod::computeM_u32(input.numFactors_);

    ExcAssert(output.size() == input.size());

    if (output.factor() != input.factor()) {
        cerr << "output.factor() = " << (int)output.factor() << endl;
        cerr << "input.factor() = " << (int)input.factor() << endl;
    }
    ExcAssert(output.factor() == input.factor());
    ExcAssert(output.numFactors() == input.numFactors());
    ExcAssert(output.is64Bits() == input.is64Bits());

    bool validate = context.getOption(VALIDATE);
    for (size_t i = 0;  i < input.size() && validate;  ++i) {
        if (input.at(i) != output.at(i)) {
            cerr << "this->at(pos) = " << output.at(i) << endl;
            cerr << "value = " << input.at(i) << endl;
            cerr << "pos = " << i << endl;
            cerr << "is64Bits() = " << output.is64Bits() << endl;
            cerr << "factor() = " << output.factor() << endl;
            cerr << "numFactors() = " << output.numFactors() << endl;

        }
        ExcAssert(input.at(i) == output.at(i));
    }
}

void freeze(MappingContext & context, MappedFactoredIntTable & output, const FactoredIntTable & input)
{
    freeze_factored_table(context, output, input, 0);
}

void freeze(MappingContext & context, InternalMappedFactoredIntTable<1> & output, const FactoredIntTable & input)
{
    freeze_factored_table(context, output, input, 1);
}

void freeze(MappingContext & context, InternalMappedFactoredIntTable<2> & output, const FactoredIntTable & input)
{
    freeze_factored_table(context, output, input, 2);
}

FactoredIntTable factored_encode(const std::span<const uint32_t> & values, bool /* trace */)
{
    FactoredIntTable result(0, 0, allow64BitFactors);
    if (values.empty()) {
        return result;
    }
    uint32_t maxValue = *std::max_element(values.begin(), values.end());

    result = FactoredIntTable(values.size(), maxValue, allow64BitFactors);

    result.set_range(values);

    for (size_t i = 0;  i < values.size();  ++i) {
        if (values[i] != result.at(i)) {
            cerr << "value " << i << " values[i] " << values[i] << " result.at(i) " << result.at(i) << endl;
        }
        ExcAssert(values[i] == result.at(i));
    }

    return result;
}

void freeze(MappingContext & context, MappedFactoredIntTable & output, const std::span<const uint32_t> & input)
{
    freeze_factored_table(context, output, factored_encode(input), 0);
}

void freeze(MappingContext & context, InternalMappedFactoredIntTable<1> & output, const std::span<const uint32_t> & input)
{
    freeze_factored_table(context, output, factored_encode(input), 1);

}

void freeze(MappingContext & context, InternalMappedFactoredIntTable<2> & output, const std::span<const uint32_t> & input)
{
    freeze_factored_table(context, output, factored_encode(input), 2);
}

FactoredIntTableSolution factored_int_table_solution(size_t size, uint32_t maxValue, bool allow64Bits)
{
    if (size == 0 || maxValue == 0)
        return { false, 1, 0, 0 };
    
    auto [unused1, factor, numFactors] = get_factor_packing_solution(maxValue);

    auto numFactorsPer64BitWord = (uint32_t)numFactors;
    auto numFactorsPer32BitWord = (uint32_t)numFactorsPer64BitWord / 2;

    auto num64BitWords = (size + (size_t)numFactorsPer64BitWord - 1) / (size_t)numFactorsPer64BitWord;
    auto num32BitWords = (size + (size_t)numFactorsPer32BitWord - 1) / (size_t)numFactorsPer32BitWord;

    // Use 64 bit accesses only if we save memory by doing so
    bool is64 = allow64Bits && num64BitWords * 8 < num32BitWords * 4;

    if (is64) {        
        return { true, factor, numFactorsPer64BitWord, num64BitWords * 8 };
    }
    else {
        return { false, factor, numFactorsPer32BitWord, num32BitWords * 4 };
    }
}

size_t factored_table_indirect_bytes(const IntTableStats<uint32_t> & stats, bool allow64Bits)
{
    return factored_int_table_solution(stats.size, stats.maxValue, allow64Bits).bytesToAllocate;
}

size_t factored_table_indirect_bytes(size_t size, uint32_t maxValue, bool allow64Bits)
{
    return factored_int_table_solution(size, maxValue, allow64Bits).bytesToAllocate;
}

template struct WritableFactoredIntTableImplT<FactoredIntTable>;
template struct FactoredIntTableImplT<MappedFactoredIntTable>;
template struct InternalMappedFactoredIntTable<1>;
template struct InternalMappedFactoredIntTable<2>;

// Ensure instantiation of methods marked inline
auto factoredAtAddress = &FactoredIntTable::at;
auto mappedFactoredAtAddress = &MappedFactoredIntTable::at;
auto mappedFactoredCountValuesAddress = &MappedFactoredIntTable::countValues;
auto mappedFactoredIndirectBytesRequiredAddress = &MappedFactoredIntTable::indirectBytesRequired;

DEFINE_STRUCTURE_DESCRIPTION_INLINE(MappedFactoredIntTable)
{
    addBitField("type", &MappedFactoredIntTable::flags_, 0, 3, "== FACTORED");
    addBitField("size", &MappedFactoredIntTable::flags_, 3, 22, "Size of the factored table (< 1024 internal, > 1024 external and must subtract 1024");
    addBitField("is64", &MappedFactoredIntTable::flags_, 25, 1, "Are we factoring 32 bit(0) or 64 bit(1) integers?");
    addBitField("numFactors", &MappedFactoredIntTable::flags_, 26, 6, "Number of factors per integer");
    addField("factor", &MappedFactoredIntTable::factor_, "Factor used in factorization");

    auto dataEnabled = [] (const void * obj) -> bool
    {
        return reinterpret_cast<const MappedFactoredIntTable *>(obj)->size_ >= MappedFactoredIntTable::INTERNAL_OFFSET;
    };

    addDiscriminatedField("data", &MappedFactoredIntTable::data_, dataEnabled, "Pointer to external data", "size_ >= 1024");
}

// Take addresses of methods to ensure instantiation even when the impl
// header is not included
auto factoredIntTableInitializeAddress = &FactoredIntTable::initialize;
auto factoredIntTableSetAddress = &FactoredIntTable::set;

} // namespace MLDB
