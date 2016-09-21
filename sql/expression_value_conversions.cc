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
            std::vector<ColumnName> columnNames;
            ML::distribution<double> d1;

            auto onAtom = [&] (const ColumnName & name,
                               const ColumnName & prefix,
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

            auto token = std::make_shared<std::vector<ColumnName> >
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
                = std::static_pointer_cast<const std::vector<ColumnName> >
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
                     StorageType bufType,
                     StorageType storageType)
{
    switch (bufType) {
    case ST_FLOAT32:
        getNumbers(from,
                   storageType,
                   (float *)to, len);
        return;
    case ST_FLOAT64:
        getNumbers(from,
                   storageType,
                   (double *)to, len);
        return;
    case ST_INT8:
        getNumbers(from,
                   storageType,
                   (int8_t *)to, len);
        return;
    case ST_UINT8:
        getNumbers(from,
                   storageType,
                   (uint8_t *)to, len);
        return;
    case ST_INT16:
        getNumbers(from,
                   storageType,
                   (int16_t *)to, len);
        return;
    case ST_UINT16:
        getNumbers(from,
                   storageType,
                   (uint16_t *)to, len);
        return;
    case ST_INT32:
        getNumbers(from,
                   storageType,
                   (int32_t *)to, len);
        return;
    case ST_UINT32:
        getNumbers(from,
                   storageType,
                   (uint32_t *)to, len);
        return;
    case ST_INT64:
        getNumbers(from,
                   storageType,
                   (int64_t *)to, len);
        return;
    case ST_UINT64:
        getNumbers(from,
                   storageType,
                   (uint64_t *)to, len);
        return;
    case ST_BLOB:
        // Unfortunately, gcc6.1.1 ICEs when enabling this
        throw HttpReturnException(600, "Not done: blob conversions");
#if 0
        getNumbers(from,
                   storageType,
                   (std::vector<uint8_t> *)to, len);
#endif
        return;
    case ST_STRING:
        getNumbers(from,
                   storageType,
                   (std::string *)to, len);
        return;
    case ST_UTF8STRING:
        getNumbers(from,
                   storageType,
                   (Utf8String *)to, len);
        return;
    case ST_ATOM:
        getNumbers(from,
                   storageType,
                   (CellValue *)to, len);
        return;
    case ST_BOOL:
        getNumbers(from,
                   storageType,
                   (bool *)to, len);
        return;
    case ST_TIMESTAMP:
        getNumbers(from,
                   storageType,
                   (Date *)to, len);
        return;
    case ST_TIMEINTERVAL:
        throw HttpReturnException(500, "Can't store time intervals");
    }
    throw HttpReturnException(500, "convertEmbedding unknown time");
}

} // namespace MLDB

