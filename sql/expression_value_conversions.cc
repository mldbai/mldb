#include "expression_value.h"
#include "mldb/types/enum_description.h"
#include "mldb/http/http_exception.h"
#include "mldb/jml/stats/distribution.h"


namespace MLDB {

DEFINE_ENUM_DESCRIPTION(StorageType);

StorageTypeDescription::
StorageTypeDescription()
{
    addValue("FLOAT32",    ST_FLOAT32,    "32 bit floating point number");
    addValue("FLOAT64",    ST_FLOAT64,    "64 bit floating point number");
    addValue("INT8",       ST_INT8,       "8 bit signed integer");
    addValue("UINT8",      ST_UINT8,      "8 bit unsigned integer");
    addValue("INT16",      ST_INT16,      "16 bit signed integer");
    addValue("UINT16",     ST_UINT16,     "16 bit unsigned integer");
    addValue("INT32",      ST_INT32,      "32 bit signed integer");
    addValue("UINT32",     ST_UINT32,     "32 bit unsigned integer");
    addValue("INT64",      ST_INT64,      "64 bit signed integer");
    addValue("UINT64",     ST_UINT64,     "64 bit unsigned integer");
    addValue("BLOB",       ST_BLOB,       "Binary blob");
    addValue("STRING",     ST_STRING,     "Ascii string");
    addValue("UTF8STRING", ST_UTF8STRING, "UTF-8 encoded string");
    addValue("ATOM",       ST_ATOM,       "Any atomic value");
    addValue("BOOL",       ST_BOOL,       "32 bit boolean value");
    addValue("TIMESTAMP",  ST_TIMESTAMP,  "Timestamp");
    addValue("INTERVAL",   ST_TIMEINTERVAL, "Time interval");
}

template<typename T>
const T & getValueT(const void * buf, size_t n)
{
    return n[reinterpret_cast<const T *>(buf)];
}

CellValue
getValue(const void * buf, StorageType storageType, size_t n)
{
    switch (storageType) {
    case ST_FLOAT32:
        return getValueT<float>(buf, n);
    case ST_FLOAT64:
        return getValueT<double>(buf, n);
    case ST_INT8:
        return getValueT<int8_t>(buf, n);
    case ST_UINT8:
        return getValueT<uint8_t>(buf, n);
    case ST_INT16:
        return getValueT<int16_t>(buf, n);
    case ST_UINT16:
        return getValueT<uint16_t>(buf, n);
    case ST_INT32:
        return getValueT<int32_t>(buf, n);
    case ST_UINT32:
        return getValueT<uint32_t>(buf, n);
    case ST_INT64:
        return getValueT<int64_t>(buf, n);
    case ST_UINT64:
        return getValueT<uint64_t>(buf, n);
    case ST_BLOB:
        return CellValue::blob(getValueT<std::string>(buf, n));
    case ST_STRING:
        return getValueT<std::string>(buf, n);
    case ST_UTF8STRING:
        return getValueT<Utf8String>(buf, n);
    case ST_ATOM:
        return getValueT<CellValue>(buf, n);
    case ST_BOOL:
        return getValueT<bool>(buf, n);
    case ST_TIMESTAMP:
        return getValueT<Date>(buf, n);
    case ST_TIMEINTERVAL:
        throw HttpReturnException(500, "Can't store time intervals");
    }
        
    throw HttpReturnException(500, "Unknown embedding storage type",
                              "storageType", storageType);
}

size_t
getCellSizeInBytes(StorageType storageType)
{
    switch (storageType) {
    case ST_FLOAT32:
        return sizeof(float);
    case ST_FLOAT64:
        return sizeof(double);
    case ST_INT8:
        return sizeof(int8_t);
    case ST_UINT8:
        return sizeof(uint8_t);
    case ST_INT16:
        return sizeof(int16_t);
    case ST_UINT16:
        return sizeof(uint16_t);
    case ST_INT32:
        return sizeof(int32_t);
    case ST_UINT32:
        return sizeof(uint32_t);
    case ST_INT64:
        return sizeof(int64_t);
    case ST_UINT64:
        return sizeof(uint64_t);
    case ST_BLOB:
        return sizeof(std::string);
    case ST_STRING:
        return sizeof(std::string);
    case ST_UTF8STRING:
        return sizeof(Utf8String);
    case ST_ATOM:
        return sizeof(CellValue);
    case ST_BOOL:
        return sizeof(bool);
    case ST_TIMESTAMP:
        return sizeof(Date);
    case ST_TIMEINTERVAL:
        throw HttpReturnException(500, "Can't store time intervals");
    }
        
    throw HttpReturnException(500, "Unknown embedding storage type",
                              "storageType", storageType);
}

#if 0
static double
getDouble(const void * buf, StorageType storageType, size_t n)
{
    switch (storageType) {
    case ST_FLOAT32:
        return getValueT<float>(buf, n);
    case ST_FLOAT64:
        return getValueT<double>(buf, n);
    case ST_INT8:
        return getValueT<int8_t>(buf, n);
    case ST_UINT8:
        return getValueT<uint8_t>(buf, n);
    case ST_INT16:
        return getValueT<int16_t>(buf, n);
    case ST_UINT16:
        return getValueT<uint16_t>(buf, n);
    case ST_INT32:
        return getValueT<int32_t>(buf, n);
    case ST_UINT32:
        return getValueT<uint32_t>(buf, n);
    case ST_INT64:
        return getValueT<int64_t>(buf, n);
    case ST_UINT64:
        return getValueT<uint64_t>(buf, n);
    case ST_ATOM:
        return getValueT<CellValue>(buf, n).coerceToNumber().toDouble();
    case ST_BOOL:
        return getValueT<bool>(buf, n);
    case ST_TIMESTAMP:
        return getValueT<Date>(buf, n).secondsSinceEpoch();
    case ST_BLOB:
    case ST_STRING:
    case ST_UTF8STRING:
    case ST_TIMEINTERVAL:
        throw HttpReturnException
            (400, "Can't extract number from embedding of type "
             + jsonEncodeStr(storageType));
    }
    
    throw HttpReturnException(500, "Unknown embedding storage type",
                              "storageType", storageType);
}
#endif

double coerceTo(Date d, double *)
{
    return d.secondsSinceEpoch();
}

float coerceTo(Date d, float *)
{
    return d.secondsSinceEpoch();
}

template<typename T>
T coerceTo(const T & t, T *)
{
    return t;
}

template<typename T>
CellValue coerceTo(const T & t, CellValue *)
{
    return t;
}

CellValue coerceTo(const CellValue & t, CellValue *)
{
    return t;
}

float coerceTo(const CellValue & v, float *)
{
    return v.toDouble();
}

double coerceTo(const CellValue & v, double *)
{
    return v.toDouble();
}

uint8_t coerceTo(const CellValue & v, uint8_t *)
{
    return v.toUInt();
}

uint16_t coerceTo(const CellValue & v, uint16_t *)
{
    return v.toUInt();
}

uint32_t coerceTo(const CellValue & v, uint32_t *)
{
    return v.toUInt();
}

uint64_t coerceTo(const CellValue & v, uint64_t *)
{
    return v.toUInt();
}

int8_t coerceTo(const CellValue & v, int8_t *)
{
    return v.toInt();
}

int16_t coerceTo(const CellValue & v, int16_t *)
{
    return v.toInt();
}

int32_t coerceTo(const CellValue & v, int32_t *)
{
    return v.toInt();
}

int64_t coerceTo(const CellValue & v, int64_t *)
{
    return v.toInt();
}

bool coerceTo(const CellValue & v, bool *)
{
    return v.toInt();
}

template<typename T>
std::vector<uint8_t> coerceTo(const T & t, std::vector<uint8_t> *)
{
    throw HttpReturnException(400, "Can't convert this type to a blob");
}

std::vector<uint8_t> coerceTo(const CellValue & v, std::vector<uint8_t> *)
{
    std::vector<uint8_t> result(v.blobLength());
    std::copy(v.blobData(),
              v.blobData() + result.size(),
              result.data());
    return result;
}

template<typename T>
std::string coerceTo(T t, std::string *)
{
    return CellValue(std::move(t)).toString();
}

std::string coerceTo(const CellValue & t, std::string *)
{
    return t.toString();
}

std::string coerceTo(std::string t, std::string *)
{
    return t;
}

std::string coerceTo(const Utf8String & v, std::string *)
{
    return v.extractAscii();
}

template<typename T>
Utf8String coerceTo(T t, Utf8String *)
{
    return CellValue(std::move(t)).toUtf8String();
}

Utf8String coerceTo(const CellValue & t, Utf8String *)
{
    return t.toUtf8String();
}

Utf8String coerceTo(std::string v, Utf8String *)
{
    return std::move(v);
}

template<typename T, typename TResult>
TResult coerceTo(const T & t, TResult *,
                 typename std::enable_if<std::is_arithmetic<T>::value && std::is_arithmetic<TResult>::value>::type * = 0)
{
    return t;
}

template<typename T, typename TReturn>
TReturn coerceTo(const T & t, TReturn *,
                 typename std::enable_if<!(std::is_arithmetic<T>::value && std::is_arithmetic<TReturn>::value)>::type * = 0)
{
    throw HttpReturnException(400, "Can't coerce non-numeric value to numeric type");
}


template<typename Tstorage, typename TOut>
static __attribute__((__noinline__))
TOut * getValuesT(const void * buf, TOut * out, size_t n)
{
    auto * tbuf = reinterpret_cast<const Tstorage *>(buf);
    for (; n > 0; --n) {
        *out++ = coerceTo(*tbuf++, (TOut *)0);
    }
    return out;
}

// Workaround for an ICE in gcc6.1.1
template<typename Tstorage>
static __attribute__((__noinline__))
std::vector<uint8_t> *
getValuesT(const void * buf, std::vector<uint8_t> * out, size_t n)
{
    auto * tbuf = reinterpret_cast<const Tstorage *>(buf);
    for (; n > 0; --n) {
        *out++ = coerceTo(*tbuf++, (std::vector<uint8_t> *)0);
    }
    return out;
}

#if 0
// Workaround for an ICE in gcc6.1.1
template<typename Tstorage>
static __attribute__((__noinline__))
bool * getValuesT(const void * buf, bool * out, size_t n)
{
    auto * tbuf = reinterpret_cast<const Tstorage *>(buf);
    for (; n > 0; --n) {
        *out++ = coerceTo(*tbuf++, (bool *)0);
    }
    return out;
}
#endif

template<typename TNumber>
__attribute__((__noinline__)) TNumber *
getNumbers(const void * buf, StorageType storageType, TNumber * out, size_t n)
{
    switch (storageType) {
    case ST_FLOAT32:
        return getValuesT<float>(buf, out, n);
    case ST_FLOAT64:
        return getValuesT<double>(buf, out, n);
    case ST_INT8:
        return getValuesT<int8_t>(buf, out, n);
    case ST_UINT8:
        return getValuesT<uint8_t>(buf, out, n);
    case ST_INT16:
        return getValuesT<int16_t>(buf, out, n);
    case ST_UINT16:
        return getValuesT<uint16_t>(buf, out, n);
    case ST_INT32:
        return getValuesT<int32_t>(buf, out, n);
    case ST_UINT32:
        return getValuesT<uint32_t>(buf, out, n);
    case ST_INT64:
        return getValuesT<int64_t>(buf, out, n);
    case ST_UINT64:
        return getValuesT<uint64_t>(buf, out, n);
    case ST_ATOM:
        return getValuesT<CellValue>(buf, out, n);
    case ST_BOOL:
        return getValuesT<bool>(buf, out, n);
    case ST_TIMESTAMP:
        return getValuesT<Date>(buf, out, n);
    case ST_BLOB:
    case ST_STRING:
    case ST_UTF8STRING:
    case ST_TIMEINTERVAL:
        throw HttpReturnException
            (400, "Can't extract number from embedding of type "
             + jsonEncodeStr(storageType));
    }
    
    throw HttpReturnException(500, "Unknown embedding storage type",
                              "storageType", storageType);
}

double * getNumbersDouble(const void * buf, StorageType storageType,
                          double * out, size_t n)
{
    return getNumbers(buf, storageType, out, n);
}

std::tuple<ExpressionValueInfo::GetCompatibleDoubleEmbeddingsFn,
           std::shared_ptr<ExpressionValueInfo>,
           ExpressionValueInfo::ReconstituteFromDoubleEmbeddingFn>
ExpressionValueInfo::
getCompatibleDoubleEmbeddings(const ExpressionValueInfo & other) const
{
    ExpressionValueInfo::GetCompatibleDoubleEmbeddingsFn get
        = [=] (const ExpressionValue & val1,
               const ExpressionValue & val2)
        -> std::tuple<DoubleDist, DoubleDist, std::shared_ptr<const void>, Date>
        {
            // This is a naive and slow, but correct, implementation.  We
            // extract the column names and the distribution from the first,
            // then get the same values for the second, and return enough to
            // reconstitute the same structure again.
            std::vector<ColumnPath> columnNames;
            distribution<double> d1;

            auto onAtom = [&] (const ColumnPath & name,
                               const ColumnPath & prefix,
                               const CellValue & val,
                               Date ts)
            {
                d1.emplace_back(coerceTo(val, (double *)0));
                columnNames.emplace_back(prefix + name);
                return true;
            };

            val1.forEachAtom(onAtom);

            DoubleDist d2
                = val2.getEmbedding(columnNames.data(), columnNames.size());

            auto token = std::make_shared<std::vector<ColumnPath> >
                (std::move(columnNames));
            Date ts = std::min(val1.getEffectiveTimestamp(),
                               val2.getEffectiveTimestamp());
            return std::make_tuple(std::move(d1), std::move(d2),
                                   std::move(token), ts);
        };

    ExpressionValueInfo::ReconstituteFromDoubleEmbeddingFn reconst
        = [=] (std::vector<double> vals,
               const std::shared_ptr<const void> & token,
               Date timestamp)
        -> ExpressionValue
        {
            ExcAssert(token);
            // Get our column names back out
            auto columnNames
                = std::static_pointer_cast<const std::vector<ColumnPath> >
                  (token);

            return ExpressionValue(std::move(vals), std::move(columnNames),
                                   timestamp);
        };
    
    // For the moment, we just return an embedding value info.  Later for
    // better safety and performance we will update.
    auto info = std::make_shared<EmbeddingValueInfo>(-1, ST_FLOAT64);
    
    return std::make_tuple(std::move(get), std::move(info), std::move(reconst));
}

void
convertEmbeddingImpl(void * to,
                     const void * from,
                     size_t len,
                     StorageType toType,
                     StorageType fromType)
{
    switch (toType) {
    case ST_FLOAT32:
        getNumbers(from,
                   fromType,
                   (float *)to, len);
        return;
    case ST_FLOAT64:
        getNumbers(from,
                   fromType,
                   (double *)to, len);
        return;
    case ST_INT8:
        getNumbers(from,
                   fromType,
                   (int8_t *)to, len);
        return;
    case ST_UINT8:
        getNumbers(from,
                   fromType,
                   (uint8_t *)to, len);
        return;
    case ST_INT16:
        getNumbers(from,
                   fromType,
                   (int16_t *)to, len);
        return;
    case ST_UINT16:
        getNumbers(from,
                   fromType,
                   (uint16_t *)to, len);
        return;
    case ST_INT32:
        getNumbers(from,
                   fromType,
                   (int32_t *)to, len);
        return;
    case ST_UINT32:
        getNumbers(from,
                   fromType,
                   (uint32_t *)to, len);
        return;
    case ST_INT64:
        getNumbers(from,
                   fromType,
                   (int64_t *)to, len);
        return;
    case ST_UINT64:
        getNumbers(from,
                   fromType,
                   (uint64_t *)to, len);
        return;
    case ST_BLOB:
        // Unfortunately, gcc6.1.1 ICEs when enabling this
        throw HttpReturnException(600, "Not done: blob conversions");
#if 0
        getNumbers(from,
                   fromType,
                   (std::vector<uint8_t> *)to, len);
#endif
        return;
    case ST_STRING:
        getNumbers(from,
                   fromType,
                   (std::string *)to, len);
        return;
    case ST_UTF8STRING:
        getNumbers(from,
                   fromType,
                   (Utf8String *)to, len);
        return;
    case ST_ATOM:
        getNumbers(from,
                   fromType,
                   (CellValue *)to, len);
        return;
    case ST_BOOL:
        getNumbers(from,
                   fromType,
                   (bool *)to, len);
        return;
    case ST_TIMESTAMP:
        getNumbers(from,
                   fromType,
                   (Date *)to, len);
        return;
    case ST_TIMEINTERVAL:
        throw HttpReturnException(500, "Can't store time intervals");
    }
    throw HttpReturnException(500, "convertEmbedding unknown time");
}

// TODO: generalize
std::shared_ptr<ExpressionValueInfo>
getValueInfoForStorage(StorageType type)
{
    switch (type) {
    case ST_FLOAT32:
        return std::make_shared<Float32ValueInfo>();
    case ST_FLOAT64:
        return std::make_shared<Float64ValueInfo>();
    case ST_INT8:
        return std::make_shared<IntegerValueInfo>();
    case ST_UINT8:
        return std::make_shared<IntegerValueInfo>();
    case ST_INT16:
        return std::make_shared<IntegerValueInfo>();
    case ST_UINT16:
        return std::make_shared<IntegerValueInfo>();
    case ST_INT32:
        return std::make_shared<IntegerValueInfo>();
    case ST_UINT32:
        return std::make_shared<IntegerValueInfo>();
    case ST_INT64:
        return std::make_shared<IntegerValueInfo>();
    case ST_UINT64:
        return std::make_shared<Uint64ValueInfo>();
    case ST_BLOB:
        return std::make_shared<BlobValueInfo>();
    case ST_STRING:
        return std::make_shared<StringValueInfo>();
    case ST_UTF8STRING:
        return std::make_shared<Utf8StringValueInfo>();
    case ST_ATOM:
        return std::make_shared<AtomValueInfo>();
    case ST_BOOL:
        return std::make_shared<BooleanValueInfo>();
    case ST_TIMESTAMP:
        return std::make_shared<TimestampValueInfo>();
    case ST_TIMEINTERVAL:
        return std::make_shared<AtomValueInfo>();
    }

    throw HttpReturnException(500, "Unknown embedding storage type",
                              "type", type);
}

StorageType valueStorageType(const CellValue & value)
{
    switch (value.cellType()) {
    case CellValue::EMPTY:
        return ST_ATOM;
    case CellValue::INTEGER:
        if (value.isPositiveNumber()) {
            uint64_t uval = value.toUInt();
            if (uval <= 256)
                return ST_UINT8;
            else if (uval < 65536)
                return ST_UINT16;
            else if (uval <= (1ULL << 32))
                return ST_UINT32;
            else return ST_UINT64;
        }
        else {
            int64_t uval = value.toInt();
            if (uval <= 256)
                return ST_UINT8;
            else if (uval < 65536)
                return ST_UINT16;
            else if (uval <= (1ULL << 32))
                return ST_UINT32;
            else return ST_UINT64;
        }

    case CellValue::FLOAT:
        return ST_FLOAT32;
    case CellValue::ASCII_STRING:
        return ST_STRING;
    case CellValue::UTF8_STRING:
        return ST_UTF8STRING;
    case CellValue::TIMESTAMP:
        return ST_TIMESTAMP;
    case CellValue::TIMEINTERVAL:
        return ST_TIMEINTERVAL;
    case CellValue::BLOB:
        return ST_BLOB;
    case CellValue::PATH:
        return ST_ATOM;
    case CellValue::NUM_CELL_TYPES:
        break;
    }

    throw HttpReturnException(500, "Unknown CellValue type for "
                              + jsonEncodeStr(value));
}

StorageType coveringStorageType(StorageType type1, StorageType type2)
{
    if (type2 < type1)
        return coveringStorageType(type2, type1);
    
    if (type1 == type2)
        return type1;
    if (type1 == ST_ATOM || type2 == ST_ATOM)
        return ST_ATOM;

    switch (type1) {

    case ST_FLOAT32:
        switch (type2) {
        case ST_FLOAT64:
            return ST_FLOAT64;
        case ST_INT8:
        case ST_UINT8:
        case ST_INT16:
        case ST_UINT16:  // up to 16 bit integers exactly representable in float32
        case ST_BOOL:
            return ST_FLOAT32;
        case ST_INT32:
        case ST_UINT32:
            return ST_FLOAT64;
        default:
            return ST_ATOM;
        }

    case ST_FLOAT64:
        switch (type2) {
        case ST_INT8:
        case ST_UINT8:
        case ST_INT16:
        case ST_UINT16:
        case ST_INT32:
        case ST_UINT32:
        case ST_BOOL:
            return ST_FLOAT64;
        default:
            return ST_ATOM;
        }
        

    case ST_INT8:
        switch (type2) {
        case ST_BOOL:
            return ST_INT8;
        case ST_UINT8:
        case ST_INT16:
            return ST_INT16;
        case ST_UINT16:
        case ST_INT32:
            return ST_INT32;
        case ST_UINT32:
        case ST_INT64:
            return ST_INT64;
        default:
            return ST_ATOM;
        }

    case ST_UINT8:
        switch (type2) {
        case ST_BOOL:
            return ST_UINT8;
        case ST_INT16:
        case ST_UINT16:
        case ST_INT32:
        case ST_UINT32:
        case ST_INT64:
        case ST_UINT64:
            return type2;
        default:
            return ST_ATOM;
        }

    case ST_INT16:
        switch (type2) {
        case ST_BOOL:
            return ST_INT16;
        case ST_UINT16:
        case ST_INT32:
            return ST_INT32;
        case ST_UINT32:
        case ST_INT64:
            return ST_INT64;
        default:
            return ST_ATOM;
        }
        
    case ST_UINT16:
        switch (type2) {
        case ST_BOOL:
            return ST_UINT16;
        case ST_INT32:
        case ST_UINT32:
        case ST_INT64:
        case ST_UINT64:
            return type2;
        default:
            return ST_ATOM;
        }

    case ST_INT32:
        switch (type2) {
        case ST_BOOL:
            return ST_INT32;
        case ST_UINT32:
        case ST_INT64:
            return ST_INT64;
        default:
            return ST_ATOM;
        }

    case ST_UINT32:
        switch (type2) {
        case ST_BOOL:
            return ST_UINT32;
        case ST_INT64:
        case ST_UINT64:
            return type2;
        default:
            return ST_ATOM;
        }
        
    case ST_INT64:
    case ST_UINT64:
        if (type2 == ST_BOOL)
            return type1;
        return ST_ATOM;
    case ST_BLOB:
        return ST_ATOM;
    case ST_STRING:
        if (type2 == ST_UTF8STRING)
            return ST_UTF8STRING;
        return ST_ATOM;
    case ST_UTF8STRING:
    case ST_ATOM:
        return ST_ATOM;
    case ST_BOOL:
        return ST_ATOM;
    case ST_TIMESTAMP:
    case ST_TIMEINTERVAL:
        return ST_ATOM;
    }
    return ST_ATOM;
}

StorageType coveringStorageType(StorageType type1, const CellValue & val2)
{
    // TODO: we can be more specific than this
    // eg, if we have UINT8 and we call with -1, we will get INT16 not
    // int8 even if values are only < 127
    return coveringStorageType(type1, valueStorageType(val2));
}

StorageType coveringStorageType(StorageType type1, const ExpressionValue & val2)
{
    return coveringStorageType(type1, val2.getAtom());
}

/** How many bytes is a single instance of the given storage type? */
size_t sizeofStorageType(StorageType type)
{
    return getCellSizeInBytes(type);
}

std::shared_ptr<void>
allocateStorageBuffer(size_t size, StorageType storage)
{
    void * ptr;
    int res = posix_memalign(&ptr, 32, storageBufferBytes(size, storage));
    if (res != 0) {
        throw std::bad_alloc();
    }
    std::shared_ptr<void> mem(ptr, free);
    initializeStorageBuffer(ptr, storage, size);
    return mem;
}

std::shared_ptr<void>
allocateStorageBuffer(const DimsVector & size, StorageType storage)
{
    size_t sz = 1;
    for (auto & s: size)
        sz *= s;
    return allocateStorageBuffer(sz, storage);
}


/** Return the size of a storage buffer of the given type.  Includes only
    direct (no indirect) storage.
*/
uint64_t storageBufferBytes(size_t size, StorageType storage)
{
    return size * getCellSizeInBytes(storage);
}

uint64_t storageBufferBytes(const DimsVector & size, StorageType storage)
{
    size_t sz = 1;
    for (auto & s: size)
        sz *= s;
    return storageBufferBytes(size, storage);
}

void * addOfs(void * p, ssize_t n)
{
    return reinterpret_cast<char *>(p) + n;
}

const void * addOfs(const void * p, ssize_t n)
{
    return reinterpret_cast<const char *>(p) + n;
}

void copyStorageBuffer(const void * from, size_t fromOffset, StorageType fromType,
                       void * to, size_t toOffset, StorageType toType,
                       size_t numElements)
{
    from = addOfs(from, storageBufferBytes(fromOffset, fromType));
    to = addOfs(to, storageBufferBytes(toOffset, toType));
    convertEmbeddingImpl(to, from, numElements, toType, fromType);
    
}

void moveStorageBuffer(void * from, size_t fromOffset, StorageType fromType,
                       void * to, size_t toOffset, StorageType toType,
                       size_t numElements)
{
    // TODO: move not copy (later)
    copyStorageBuffer(from, fromOffset, fromType,
                      to, toOffset, toType,
                      numElements);
}

template<typename T>
void fillBuffer(T * buf, size_t numElements, const CellValue & val)
{
    std::fill(buf, buf + numElements, coerceTo(val, (T *)0));
}

void fillStorageBuffer(void * buffer, size_t offset, StorageType storageType,
                       size_t numElements, const CellValue & val)
{
    buffer = addOfs(buffer, storageBufferBytes(offset, storageType));
    
    switch (storageType) {
    case ST_FLOAT32:
        fillBuffer((float *)buffer, numElements, val);
        return;
    case ST_FLOAT64:
        fillBuffer((double *)buffer, numElements, val);
        return;
    case ST_INT8:
        fillBuffer((int8_t *)buffer, numElements, val);
        return;
    case ST_UINT8:
        fillBuffer((uint8_t *)buffer, numElements, val);
        return;
    case ST_INT16:
        fillBuffer((int16_t *)buffer, numElements, val);
        return;
    case ST_UINT16:
        fillBuffer((uint16_t *)buffer, numElements, val);
        return;
    case ST_INT32:
        fillBuffer((int32_t *)buffer, numElements, val);
        return;
    case ST_UINT32:
        fillBuffer((uint32_t *)buffer, numElements, val);
        return;
    case ST_INT64:
        fillBuffer((int64_t *)buffer, numElements, val);
        return;
    case ST_UINT64:
        fillBuffer((uint64_t *)buffer, numElements, val);
        return;
    case ST_BLOB:
        // Unfortunately, gcc6.1.1 ICEs when enabling this
        throw HttpReturnException(600, "Not done: blob conversions");
#if 0
        fillBuffer((std::vector<uint8_t> *)buffer, numElements, val);
#endif
        return;
    case ST_STRING:
        fillBuffer((std::string *)buffer, numElements, val);
        return;
    case ST_UTF8STRING:
        fillBuffer((Utf8String *)buffer, numElements, val);
        return;
    case ST_ATOM:
        fillBuffer((CellValue *)buffer, numElements, val);
        return;
    case ST_BOOL:
        fillBuffer((bool *)buffer, numElements, val);
        return;
    case ST_TIMESTAMP:
        fillBuffer((Date *)buffer, numElements, val);
        return;
    case ST_TIMEINTERVAL:
        throw HttpReturnException(500, "Can't store time intervals");
    }
    throw HttpReturnException(500, "convertEmbedding unknown time");
}

template<typename T>
void initializeBuffer(T * buf, size_t numElements, const CellValue & val)
{
    std::uninitialized_fill(buf, buf + numElements, coerceTo(val, (T *)0));
}

void initializeStorageBuffer(void * buffer, StorageType storageType,
                             size_t numElements, const CellValue & val)
{
    switch (storageType) {
    case ST_FLOAT32:
        initializeBuffer((float *)buffer, numElements, val);
        return;
    case ST_FLOAT64:
        initializeBuffer((double *)buffer, numElements, val);
        return;
    case ST_INT8:
        initializeBuffer((int8_t *)buffer, numElements, val);
        return;
    case ST_UINT8:
        initializeBuffer((uint8_t *)buffer, numElements, val);
        return;
    case ST_INT16:
        initializeBuffer((int16_t *)buffer, numElements, val);
        return;
    case ST_UINT16:
        initializeBuffer((uint16_t *)buffer, numElements, val);
        return;
    case ST_INT32:
        initializeBuffer((int32_t *)buffer, numElements, val);
        return;
    case ST_UINT32:
        initializeBuffer((uint32_t *)buffer, numElements, val);
        return;
    case ST_INT64:
        initializeBuffer((int64_t *)buffer, numElements, val);
        return;
    case ST_UINT64:
        initializeBuffer((uint64_t *)buffer, numElements, val);
        return;
    case ST_BLOB:
        // Unfortunately, gcc6.1.1 ICEs when enabling this
        throw HttpReturnException(600, "Not done: blob conversions");
#if 0
        initializeBuffer((std::vector<uint8_t> *)buffer, numElements, val);
#endif
        return;
    case ST_STRING:
        initializeBuffer((std::string *)buffer, numElements, val);
        return;
    case ST_UTF8STRING:
        initializeBuffer((Utf8String *)buffer, numElements, val);
        return;
    case ST_ATOM:
        initializeBuffer((CellValue *)buffer, numElements, val);
        return;
    case ST_BOOL:
        initializeBuffer((bool *)buffer, numElements, val);
        return;
    case ST_TIMESTAMP:
        initializeBuffer((Date *)buffer, numElements, val);
        return;
    case ST_TIMEINTERVAL:
        throw HttpReturnException(500, "Can't store time intervals");
    }
    throw HttpReturnException(500, "convertEmbedding unknown time");
}

template<typename T>
void defaultConstructBuffer(T * buf, size_t numElements)
{
    std::uninitialized_fill(buf, buf + numElements, T());
}

void initializeStorageBuffer(void * buffer, StorageType storageType,
                             size_t numElements)
{
    switch (storageType) {
    case ST_FLOAT32:
        defaultConstructBuffer((float *)buffer, numElements);
        return;
    case ST_FLOAT64:
        defaultConstructBuffer((double *)buffer, numElements);
        return;
    case ST_INT8:
        defaultConstructBuffer((int8_t *)buffer, numElements);
        return;
    case ST_UINT8:
        defaultConstructBuffer((uint8_t *)buffer, numElements);
        return;
    case ST_INT16:
        defaultConstructBuffer((int16_t *)buffer, numElements);
        return;
    case ST_UINT16:
        defaultConstructBuffer((uint16_t *)buffer, numElements);
        return;
    case ST_INT32:
        defaultConstructBuffer((int32_t *)buffer, numElements);
        return;
    case ST_UINT32:
        defaultConstructBuffer((uint32_t *)buffer, numElements);
        return;
    case ST_INT64:
        defaultConstructBuffer((int64_t *)buffer, numElements);
        return;
    case ST_UINT64:
        defaultConstructBuffer((uint64_t *)buffer, numElements);
        return;
    case ST_BLOB:
        // Unfortunately, gcc6.1.1 ICEs when enabling this
        throw HttpReturnException(600, "Not done: blob conversions");
#if 0
        defaultConstructBuffer((std::vector<uint8_t> *)buffer, numElements);
#endif
        return;
    case ST_STRING:
        defaultConstructBuffer((std::string *)buffer, numElements);
        return;
    case ST_UTF8STRING:
        defaultConstructBuffer((Utf8String *)buffer, numElements);
        return;
    case ST_ATOM:
        defaultConstructBuffer((CellValue *)buffer, numElements);
        return;
    case ST_BOOL:
        defaultConstructBuffer((bool *)buffer, numElements);
        return;
    case ST_TIMESTAMP:
        defaultConstructBuffer((Date *)buffer, numElements);
        return;
    case ST_TIMEINTERVAL:
        throw HttpReturnException(500, "Can't store time intervals");
    }
    throw HttpReturnException(500, "convertEmbedding unknown time");
}

} // namespace MLDB

