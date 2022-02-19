#include <stdint.h>
#include <stddef.h>
#include <cassert>
#include "mapped_int_table.hpp"
#include "raw_mapped_int_table.hpp"
#include "run_length_int_table.hpp"
#include "factored_int_table.hpp"
#include "bit_compressed_int_table.hpp"
#include "int_table_test_utils.hpp"

bool traceTests = false;

template<typename Container, size_t N>
void testContainer(const std::span<const uint32_t> & input)
{
    ExcAssert((input[0] & 0xff) == N);
    static_assert(TypeTestIndex<Container>::val == N);
    MappingContext context(1000 << 10, DUMP_TYPE_STATS=traceTests, INT_TABLE_TRACE_LEVEL=traceTests ? 5 : 0);
    auto & output = context.alloc<Container>();
    freeze(context, output, input.subspan(1));
    ExcAssert(input.size() - 1 == output.size());
    for (size_t i = 0;  i < input.size() - 1;  ++i) {
        ExcAssert(input[i + 1] == output.at(i));
    }
}

extern "C" int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size)
{
    if (size < 8)
        return 0;

    std::span<const uint32_t> input((const uint32_t *)data, size / 4);

    if (data[0] == 0) {
        testContainer<MappedIntTable<uint32_t>, 0>(input);
    }
    if (data[0] == 1) {
        testContainer<RawMappedIntTable, 1>(input);
    }
    if (data[0] == 2) {
        testContainer<MappedRunLengthIntTable, 2>(input);
    }
    if (data[0] == 3) {
        testContainer<MappedFactoredIntTable, 3>(input);
    }
    if (data[0] == 4) {
        testContainer<MappedBitCompressedIntTable, 4>(input);
    }
    if (data[0] == 64) {
        testContainer<InternalMappedBitCompressedIntTable<1>, 64>(input);
    }
    if (data[0] == 65) {
        testContainer<InternalMappedBitCompressedIntTable<2>, 65>(input);
    }
    if (data[0] == 66) {
        testContainer<InternalMappedBitCompressedIntTable<3>, 66>(input);
    }
    if (data[0] == 96) {
        testContainer<InternalMappedFactoredIntTable<1>, 96>(input);
    }
    if (data[0] == 97) {
        testContainer<InternalMappedFactoredIntTable<2>, 97>(input);
    }
    return 0;
}
