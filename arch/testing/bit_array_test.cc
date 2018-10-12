/* bit_array_test.cc
   Jeremy Barnes, 21 February 2007
   Copyright (c) 2007 Jeremy Barnes.  All rights reserved.
   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

   Benchmark of SIMD vector operations.
*/

#define BOOST_TEST_MAIN
#define BOOST_TEST_DYN_LINK

#include "mldb/arch/bit_array.h"
#include "mldb/arch/bit_range_ops.h"
#include "mldb/arch/timers.h"
#include "mldb/arch/demangle.h"
#include <boost/test/unit_test.hpp>
#include <cmath>

using namespace MLDB;
using namespace std;

using boost::unit_test::test_suite;

template<typename T>
struct BitExtractorArray {
    int numBits;
    BitArray<T> array;

    BitExtractorArray(int numBits)
        : numBits(numBits),
          array(numBits)
    {
    }

    T createMask() const { return array.createMask(); }

    size_t numWordsToAllocate(size_t numElements) const
    {
        return array.numWordsToAllocate(numElements);
    }

    struct Writer {
        Writer(int numBits, T * data)
            : numBits(numBits), writer(data)
        {
        }

        int numBits;
        Bit_Writer<T> writer;

        void write(T data)
        {
            writer.write(data, numBits);
        }

        void finish()
        {
        }
    };

    Writer getWriter(T * data)
    {
        return Writer(numBits, data);
    }

    struct Reader {
        Reader(int numBits, const T * data)
            : numBits(numBits), reader(data)
        {
        }

        int numBits;
        Bit_Extractor<T> reader;

        T read()
        {
            return reader.template extractFast<T>(numBits);
        }

        T readUnmasked()
        {
            return reader.template extractFastUnmasked<T>(numBits);
        }
    };

    Reader getReader(const T * data)
    {
        return Reader(numBits, data);
    }
};

template<typename T>
struct PlainArray {
    PlainArray(int numBits)
    {
    }

    T createMask() const { return (T)-1; }

    size_t numWordsToAllocate(size_t numElements) const
    {
        return numElements;
    }

    struct Writer {
        Writer(T * data)
            : data(data)
        {
        }
        
        T * data;
        
        void write(T val)
        {
            *data++ = val;
        }

        void finish()
        {
        }
    };

    Writer getWriter(T * data)
    {
        return Writer(data);
    }

    struct Reader {
        Reader(const T * data)
            : data(data)
        {
        }

        const T * data;

        T read()
        {
            return *data++;
        }

        T readUnmasked()
        {
            return *data++;
        }
    };

    Reader getReader(const T * data)
    {
        return Reader(data);
    }
};

template<typename T>
struct ByteExtractorArray {
    int numBytes;

    ByteExtractorArray(int numBits)
        : numBytes((numBits + 7) / 8)
    {
    }

    T createMask() const { return numBytes == sizeof(T) ? -1 : (1 << (numBytes * 8)) - 1; }

    size_t numWordsToAllocate(size_t numElements) const
    {
        return (numBytes * numElements + sizeof(T) - 1) / sizeof(T);
    }

    struct Writer {
        Writer(int numBytes, T * data)
            : numBytes(numBytes), data(data)
        {
        }

        int numBytes;
        T * data;
        
        void write(T val)
        {
            *data |= val;
            data = (T *)(((char *)data) + numBytes);
        }

        void finish()
        {
        }
    };

    Writer getWriter(T * data)
    {
        return Writer(numBytes, data);
    }

    struct Reader {
        Reader(int numBytes, const T * data)
            : numBytes(numBytes), data(data),
              mask(numBytes == sizeof(T) ? -1 : (((T)1 << numBytes * 8) - 1))
        {
        }

        int numBytes;
        const T * data;
        T mask;
        
        T readUnmasked()
        {
            T result = *((T *)data);
            data = (const T *)(((const char *)data) + numBytes);
            return result;
        }

        T read()
        {
            return readUnmasked() & mask;
        }
    };

    Reader getReader(const T * data)
    {
        return Reader(numBytes, data);
    }
};


BOOST_AUTO_TEST_CASE( test_bit_array_aligned_32 )
{
    BitArray<uint32_t> array(32);  // 32 bit array
    uint32_t vals[32] = { 6, 6, 6, 6, 6, 6 };

    auto writer = array.getWriter(vals);
    writer.write(1);
    BOOST_CHECK_EQUAL(vals[0], 1);
    writer.write(2);
    BOOST_CHECK_EQUAL(vals[1], 2);
    writer.write(3);
    BOOST_CHECK_EQUAL(vals[2], 3);
    writer.write(4);
    BOOST_CHECK_EQUAL(vals[3], 4);
    writer.write(5);
    BOOST_CHECK_EQUAL(vals[4], 5);
    writer.finish();
    BOOST_CHECK_EQUAL(vals[5], 0);
    

    auto reader = array.getReader(vals);
    BOOST_CHECK_EQUAL(reader.read(), 1);
    BOOST_CHECK_EQUAL(reader.read(), 2);
    BOOST_CHECK_EQUAL(reader.read(), 3);
    BOOST_CHECK_EQUAL(reader.read(), 4);
    BOOST_CHECK_EQUAL(reader.read(), 5);
}

BOOST_AUTO_TEST_CASE( test_bit_array_aligned_64 )
{
    BitArray<uint64_t> array(64);  // 64 bit array
    uint64_t vals[64] = { 6, 6, 6, 6, 6, 6 };

    auto writer = array.getWriter(vals);
    writer.write(1);
    BOOST_CHECK_EQUAL(vals[0], 1);
    writer.write(2);
    BOOST_CHECK_EQUAL(vals[1], 2);
    writer.write(3);
    BOOST_CHECK_EQUAL(vals[2], 3);
    writer.write(4);
    BOOST_CHECK_EQUAL(vals[3], 4);
    writer.write(5);
    BOOST_CHECK_EQUAL(vals[4], 5);
    writer.finish();
    BOOST_CHECK_EQUAL(vals[5], 0);
    

    auto reader = array.getReader(vals);
    BOOST_CHECK_EQUAL(reader.read(), 1);
    BOOST_CHECK_EQUAL(reader.read(), 2);
    BOOST_CHECK_EQUAL(reader.read(), 3);
    BOOST_CHECK_EQUAL(reader.read(), 4);
    BOOST_CHECK_EQUAL(reader.read(), 5);
}

BOOST_AUTO_TEST_CASE( test_bit_array )
{
    std::vector<uint32_t> data = { 1, 2, 3, 4, 5 };

    for (int i = 3;  i < 32;  ++i) {
        cerr << "length " << i << endl;
        BitArray<uint32_t> array(i);  // 32 bit array
        std::vector<uint32_t> buf(array.numWordsToAllocate(data.size()) + 1, -1);

        auto writer = array.getWriter(buf.data());
        for (auto v: data) {
            writer.write(v);
        }
        writer.finish();
        
        auto reader = array.getReader(buf.data());
        for (auto v: data) {
            BOOST_CHECK_EQUAL(reader.read(), v);
        }

        BOOST_CHECK_EQUAL(buf.back(), -1);
    }
}

BOOST_AUTO_TEST_CASE( test_bit_array_full_width_32 )
{
    size_t numEntries = 100;
    
    for (int i = 0;  i < 32;  ++i) {
        cerr << "length " << i << endl;

        BitArray<uint32_t> array(i);  // 32 bit array
        
        std::vector<uint32_t> buf(array.numWordsToAllocate(numEntries) + 1, -1);

        std::vector<uint32_t> data;
        data.resize(numEntries);
        for (int j = 0;  j < numEntries;  ++j) {
            data[j] = random() & array.createMask(i);
        }
        
        auto writer = array.getWriter(buf.data());
        for (auto v: data) {
            writer.write(v);
        }
        writer.finish();
        
        auto reader = array.getReader(buf.data());
        for (auto v: data) {
            BOOST_CHECK_EQUAL(reader.read(), v);
        }

        BOOST_CHECK_EQUAL(buf.back(), -1);
    }
}

BOOST_AUTO_TEST_CASE( test_bit_array_full_width_64 )
{
    size_t numEntries = 100;
    
    for (int i = 0;  i < 64;  ++i) {
        cerr << "length " << i << endl;

        BitArray<uint64_t> array(i);  // 64 bit array
        
        std::vector<uint64_t> buf(array.numWordsToAllocate(numEntries) + 1, -1);

        std::vector<uint64_t> data;
        data.resize(numEntries);
        for (int j = 0;  j < numEntries;  ++j) {
            data[j] = random() & array.createMask(i);
        }
        
        auto writer = array.getWriter(buf.data());
        for (auto v: data) {
            writer.write(v);
        }
        writer.finish();
        
        auto reader = array.getReader(buf.data());
        for (auto v: data) {
            BOOST_CHECK_EQUAL(reader.read(), v);
        }

        BOOST_CHECK_EQUAL(buf.back(), -1);
    }
}

#define MLDB_NEVER_INLINE __attribute__((__noinline__))


template<typename T, typename Array>
MLDB_NEVER_INLINE
void runWriteBenchmark(Array & array, T * buf, size_t numEntries)
{
    auto mask = array.createMask();

    auto writer = array.getWriter(buf);

    for (size_t i = 0;  i < numEntries;  ++i) {
        writer.write(i & mask);
    }
    writer.finish();
    
}

template<typename T, typename Array>
MLDB_NEVER_INLINE
void runReadBenchmark(Array & array, T * buf, size_t numEntries)
{
    auto reader = array.getReader(buf);
    
    int errors = 0;
    for (size_t i = 0;  i < numEntries;  ++i) {
        T val = reader.read();
        if (val != i) {
            ++errors;
        }
    }
    
    if (errors == 12345) {
        BOOST_CHECK_EQUAL(errors, 0);
    }
}

template<typename T, typename Array>
MLDB_NEVER_INLINE
void runUnmaskedReadBenchmark(Array & array, T * buf, size_t numEntries)
{
    auto mask = array.createMask();
    
    auto reader = array.getReader(buf);
    
    int errors = 0;
    for (size_t i = 0;  i < numEntries;  ++i) {
        T val = reader.readUnmasked();
        if (val != mask) {
            ++errors;
        }
        //if (val & mask != i & mask) {
        //    ++errors;
        //}
    }
    
    if (errors == 12345) {
        BOOST_CHECK_EQUAL(errors, 0);
    }
}


template<typename T, typename Array>
void
runBenchmark(size_t numBits, size_t numEntries, size_t numIter)
{
    Array array(numBits);

    size_t totalWords = array.numWordsToAllocate(numEntries);
    
    T * buf = new T[totalWords];

    size_t totalBytes = totalWords * sizeof(T);

    double cpu_write = INFINITY;
    double ticks_write = INFINITY;
    double cpu_read = INFINITY;
    double ticks_read = INFINITY;
    double cpu_readu = INFINITY;
    double ticks_readu = INFINITY;

    for (size_t i = 0;  i < numIter;  ++i) {
    
        Timer timer;

        {
            timer.restart();
            
            runWriteBenchmark(array, buf, numEntries);
            
            cpu_write = std::min(cpu_write, timer.elapsed_cpu());
            ticks_write = std::min(ticks_write, timer.elapsed_ticks());
        }

        {
            timer.restart();

            runReadBenchmark(array, buf, numEntries);
            
            cpu_read = std::min(cpu_read, timer.elapsed_cpu());
            ticks_read = std::min(ticks_read, timer.elapsed_ticks());
        }

        {
            timer.restart();

            runUnmaskedReadBenchmark(array, buf, numEntries);
            
            cpu_readu = std::min(cpu_readu, timer.elapsed_cpu());
            ticks_readu = std::min(ticks_readu, timer.elapsed_ticks());
        }
    }

    cerr << "  testing " << numEntries << " entries of " << numBits << " bits"
         << " with " << type_name<Array>() << " and " << totalBytes / 1000000.0
         << "MB" << endl;
    
    cerr << "  write: " << cpu_write << "s at "
         << numEntries / 1000000.0 / cpu_write << " ME/s and "
         << ticks_write / numEntries << " ticks/entry" << endl;
    
    cerr << "  read: " << cpu_read << "s at "
         << numEntries / 1000000.0 / cpu_read << " ME/s and "
         << ticks_read / numEntries << " ticks/entry" << endl;

    cerr << "  read unmasked: " << cpu_readu << "s at "
         << numEntries / 1000000.0 / cpu_readu << " ME/s and "
         << ticks_readu / numEntries << " ticks/entry" << endl;
    
    delete[] buf;
}

BOOST_AUTO_TEST_CASE(benchmark)
{
    for (int i: { 1, 2, 3, 4, 5, 7, 8, 12, 15, 16, 24, 29, 32 ,33, 40, 48, 53, 63, 64}) {
        cerr << "length " << i << endl;

        //runBenchmark<uint64_t, BitArray<uint64_t> >(i, 1000);
        runBenchmark<uint64_t, BitArray<uint64_t> >(i, 10000, 1000);
        //runBenchmark<uint64_t, BitArray<uint64_t> >(i, 1000000);
        //runBenchmark<uint64_t, BitArray<uint64_t> >(i, 10000000);
        runBenchmark<uint64_t, BitArray<uint64_t> >(i, 10000000, 5);
        cerr << endl;

        //runBenchmark<uint64_t, BitExtractorArray<uint64_t> >(i, 1000);
        runBenchmark<uint64_t, BitExtractorArray<uint64_t> >(i, 10000, 1000);
        //runBenchmark<uint64_t, BitExtractorArray<uint64_t> >(i, 100000);
        //runBenchmark<uint64_t, BitExtractorArray<uint64_t> >(i, 1000000);
        runBenchmark<uint64_t, BitExtractorArray<uint64_t> >(i, 10000000, 5);
        cerr << endl;

        //runBenchmark<uint64_t, PlainArray<uint64_t> >(i, 1000);
        runBenchmark<uint64_t, PlainArray<uint64_t> >(i, 10000, 1000);
        //runBenchmark<uint64_t, PlainArray<uint64_t> >(i, 100000);
        //runBenchmark<uint64_t, PlainArray<uint64_t> >(i, 1000000);
        runBenchmark<uint64_t, PlainArray<uint64_t> >(i, 10000000, 5);
        cerr << endl;
        
        //runBenchmark<uint64_t, ByteExtractorArray<uint64_t> >(i, 1000);
        runBenchmark<uint64_t, ByteExtractorArray<uint64_t> >(i, 10000, 1000);
        //runBenchmark<uint64_t, ByteExtractorArray<uint64_t> >(i, 100000);
        //runBenchmark<uint64_t, ByteExtractorArray<uint64_t> >(i, 1000000);
        runBenchmark<uint64_t, ByteExtractorArray<uint64_t> >(i, 10000000, 5);
        cerr << endl;
        cerr << endl;
    }
}
