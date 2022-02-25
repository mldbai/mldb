/* bit_compressed_bit_table.h                                              -*- C++ -*-
   Jeremy Barnes, 6 November 2015
   Copyright (c) 2015 mldb.ai inc.  All rights reserved.

   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
*/

#include "bit_compressed_int_table.h"
#include "bit_compressed_int_table_impl.h"
#include "mldb/types/value_description.h"
#include "mldb/types/list_description_base.h"
#include <functional>

namespace MLDB {

using namespace std;

uint32_t BitCompressedIntTable::at(uint32_t pos) const
{
    return impl().at(pos);
}

uint32_t BitCompressedIntTable::countValues(uint32_t startPos, uint32_t endPos, uint32_t value) const
{
    return impl().countValues(startPos, endPos, value);
}

void BitCompressedIntTable::set(uint32_t pos, uint32_t value)
{
    impl().set(pos, value);
}

uint32_t MappedBitCompressedIntTable::at(uint32_t pos) const
{
    return impl().at(pos);
}

uint32_t MappedBitCompressedIntTable::countValues(uint32_t startPos, uint32_t endPos, uint32_t value) const
{
    return impl().countValues(startPos, endPos, value);
}

MappingOption<size_t> BIT_COMPRESSED_INTERNAL_WORDS("BIT_COMPRESSED_INTERNAL_WORDS", 0);

void freeze_bit_compressed(MappingContext && context, MappedBitCompressedIntTable & output, const BitCompressedIntTable & input)
{
    auto internalWords = context.getOption(BIT_COMPRESSED_INTERNAL_WORDS);

    output.width_ = input.width_;

    constexpr size_t WORD_BITS = sizeof(uint32_t) * 8;
    uint64_t totalBits = input.size() * input.width();
    size_t totalWords = (totalBits + WORD_BITS - 1) / WORD_BITS;

    //cerr << "freezing: size " << input.size() << " internalWords " << internalWords << " totalWords " << totalWords << endl;

    // If we can fit internally we can take over a) the pointer and b) all of the internal words
    // so we have output.internalWords() + 1 words to store in.
    if (totalWords > internalWords + 1 || input.size() >= MappedBitCompressedIntTable::INTERNAL_OFFSET) {
        output.size_ = MappedBitCompressedIntTable::INTERNAL_OFFSET + input.size();
        output.data_.set(context.array_field("data", input.data_.data(), input.data_.size()));    
    }
    else {
        output.size_ = input.size_;
        for (size_t i = 0;  i < totalWords;  ++i) {
            output.internal_[i] = input.getWord(i);
        }
    }
}

void freeze(MappingContext & context, MappedBitCompressedIntTable & output, const BitCompressedIntTable & input)
{
    freeze_bit_compressed(context.with(BIT_COMPRESSED_INTERNAL_WORDS = 0), output, input);
}

template<size_t N>
void freeze_internal_bit_compressed(MappingContext & context, InternalMappedBitCompressedIntTable<N> & output, const BitCompressedIntTable & input)
{
    freeze_bit_compressed(context.with(BIT_COMPRESSED_INTERNAL_WORDS = N), output, input);
}

void freeze(MappingContext & context, InternalMappedBitCompressedIntTable<1> & output, const BitCompressedIntTable & input)
{
    freeze_internal_bit_compressed(context, output, input);
}

void freeze(MappingContext & context, InternalMappedBitCompressedIntTable<2> & output, const BitCompressedIntTable & input)
{
    freeze_internal_bit_compressed(context, output, input);
}

void freeze(MappingContext & context, InternalMappedBitCompressedIntTable<3> & output, const BitCompressedIntTable & input)
{
    freeze_internal_bit_compressed(context, output, input);
}

BitCompressedIntTable bit_compressed_encode(const std::span<const uint32_t> & values)
{
    BitCompressedIntTable result;
    if (values.empty()) {
        return result;
    }
    uint32_t maxValue = *std::max_element(values.begin(), values.end());
    uint32_t bitsPerValue = bits(maxValue);

    result.impl().initialize(values.size(), bitsPerValue);
    ExcAssert(result.impl().size() == values.size());
    ExcAssert(result.size() == values.size());
    ExcAssert(result.impl().width() == bitsPerValue);
    ExcAssert(result.width() == bitsPerValue);
    result.impl().set_range(values.begin(), values.end());
    return result;
}

void freeze(MappingContext & context, MappedBitCompressedIntTable & output, const std::span<const uint32_t> & input)
{
    freeze(context, output, bit_compressed_encode(input));
}

void freeze(MappingContext & context, InternalMappedBitCompressedIntTable<1> & output, const std::span<const uint32_t> & input)
{
    freeze(context, output, bit_compressed_encode(input));
}

void freeze(MappingContext & context, InternalMappedBitCompressedIntTable<2> & output, const std::span<const uint32_t> & input)
{
    freeze(context, output, bit_compressed_encode(input));
}

void freeze(MappingContext & context, InternalMappedBitCompressedIntTable<3> & output, const std::span<const uint32_t> & input)
{
    freeze(context, output, bit_compressed_encode(input));
}

size_t MappedBitCompressedIntTable::indirectBytesRequired(const IntTableStats<uint32_t> & stats)
{
    size_t result = bit_compressed_indirect_bytes(stats);
    if (result <= sizeof(uint32_t)) {
        result = 0;
    }
    return result;
}

template<size_t N>
size_t InternalMappedBitCompressedIntTable<N>::indirectBytesRequired(const IntTableStats<uint32_t> & stats)
{
    size_t result = bit_compressed_indirect_bytes(stats);
    if (result <= sizeof(uint32_t) * (N + 1)) {
        result = 0;
    }
    return result;
}

size_t bit_compressed_indirect_bytes(const IntTableStats<uint32_t> & stats)
{
    return bit_compressed_indirect_bytes(stats.size, stats.maxValue);
}

size_t bit_compressed_indirect_bytes(size_t size, uint32_t maxValue)
{
    return bit_compressed_indirect_bytes<uint32_t>(size, maxValue);
}

template struct WritableBitCompressedIntTableImplT<BitCompressedIntTable>;
template struct BitCompressedIntTableImplT<MappedBitCompressedIntTable>;
template struct InternalMappedBitCompressedIntTable<1>;
template struct InternalMappedBitCompressedIntTable<2>;
template struct InternalMappedBitCompressedIntTable<3>;

#if 0

struct BitField {
    const TabularTypeInfo * info = nullptr;
    const char * name = nullptr;
    int width = 0;
};

template<typename Containing, typename Underlying>
struct BitFieldBuilder {
    std::vector<BitField> fields;

    template<typename T>
    void addField(const char * name, int width)
    {
        fields.emplace_back(BitField{&getTabularTypeInfo((T*)0), name, width});
    }

    std::function<void (BitFieldBuilder &)> populate;
    ~BitFieldBuilder()
    {
        populate(*this);
    }
};

#define BIT_FIELDS(bits) \
BitFieldBuilder<ThisType, typeof(ThisType::bits)> type_builder_##bits; \
type_builder_##bits.populate = [] ([[maybe_unused]] BitFieldBuilder<ThisType, typeof(ThisType::bits)> & builder)

#define FIELD(name, width) \
builder.addField<typeof(ThisType::name)>(#name, width)

DEFINE_STRUCT_INFO(MappedBitCompressedIntTable)
{
    BIT_FIELDS(bits_) {
        FIELD(type_, 3);
        FIELD(size_, 23);
        FIELD(width_, 6);
    };
    //STRUCT_FIELD(type_);
}

#endif

#if 0
struct MappedBitCompressedIntTableDescription
    : public ValueDescriptionI<MappedBitCompressedIntTable, ValueKind::ARRAY,
                               MappedBitCompressedIntTableDescription>,
      public ListDescriptionBase<uint32_t> {

    using List = MappedBitCompressedIntTable;

    MappedBitCompressedIntTableDescription()
        : ListDescriptionBase<uint32_t>(getDefaultDescriptionSharedT<uint32_t>())
    {
    }

    MappedBitCompressedIntTableDescription(ConstructOnly)
    {
    }

    virtual void parseJson(void * val,
                           JsonParsingContext & context) const override
    {
        MLDB_THROW_UNIMPLEMENTED();
    }

    virtual void parseJsonTyped(List * val,
                                JsonParsingContext & context) const override
    {
        MLDB_THROW_UNIMPLEMENTED();
    }

    virtual void printJson(const void * val,
                           JsonPrintingContext & context) const override
    {
        const List * val2 = reinterpret_cast<const List *>(val);
        return printJsonTyped(val2, context);
    }

    virtual void printJsonTyped(const List * val,
                                JsonPrintingContext & context) const override
    {
        this->printJsonTypedList(val, context);
    }

    virtual bool isDefault(const void * val) const override
    {
        const List * val2 = reinterpret_cast<const List *>(val);
        return isDefaultTyped(val2);
    }

    virtual bool
    isDefaultTyped(const List * val) const override
    {
        return val->empty();
    }

    virtual size_t getArrayLength(void * val) const override
    {
        const List * val2 = reinterpret_cast<const List *>(val);
        return val2->size();
    }

    // TODO HACK: should be able to return by value...
    static thread_local uint32_t getArrayElementResult;

    virtual void * getArrayElement(void * val, uint32_t element) const override
    {
        List * val2 = reinterpret_cast<List *>(val);
        getArrayElementResult = val2->at(element);
        return &getArrayElementResult;
    }

    virtual const void * getArrayElement(const void * val,
                                         uint32_t element) const override
    {
        const List * val2 = reinterpret_cast<const List *>(val);
        getArrayElementResult = val2->at(element);
        return &getArrayElementResult;
    }

    virtual void setArrayLength(void * val, size_t newLength) const override
    {
        MLDB_THROW_UNIMPLEMENTED();
    }
    
    virtual const ValueDescription & contained() const override
    {
        return *this->inner;
    }

    virtual std::shared_ptr<const ValueDescription> containedPtr() const
    {
        return this->inner;
    }

    virtual void initialize() override
    {
        this->inner = getDefaultDescriptionSharedT<uint32_t>();
    }
};

DEFINE_VALUE_DESCRIPTION(MappedBitCompressedIntTable, MappedBitCompressedIntTableDescription);

thread_local uint32_t MappedBitCompressedIntTableDescription::getArrayElementResult;

#endif

DEFINE_STRUCTURE_DESCRIPTION_INLINE(MappedBitCompressedIntTable)
{
    addBitFieldCast<IntTableType>("type", &MappedBitCompressedIntTable::flags_, 0, 3, "== FACTORED");
    addBitField("size", &MappedBitCompressedIntTable::flags_, 3, 23, "Size of the factored table (< 1024 internal, > 1024 external and must subtract 1024");
    addBitField("width", &MappedBitCompressedIntTable::flags_, 26, 6, "Bit width per entry");
}

} // namespace MLDB