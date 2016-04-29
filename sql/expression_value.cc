/** expression_value.h                                             -*- C++ -*-
    Jeremy Barnes, 14 February 2015
    Copyright (c) 2015 Datacratic Inc.  All rights reserved.

    This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

    Code for the type that holds the value of an expression.
*/

#include "expression_value.h"
#include "sql_expression.h"
#include "mldb/types/structure_description.h"
#include "mldb/types/enum_description.h"
#include "mldb/types/vector_description.h"
#include "mldb/types/tuple_description.h"
#include "ml/value_descriptions.h"
#include "mldb/http/http_exception.h"
#include "mldb/jml/stats/distribution.h"
#include "mldb/utils/json_utils.h"
#include "mldb/types/hash_wrapper_description.h"
#include "mldb/jml/utils/less.h"
#include "mldb/jml/utils/lightweight_hash.h"

using namespace std;


namespace Datacratic {
namespace MLDB {

DEFINE_ENUM_DESCRIPTION(ColumnSparsity);

ColumnSparsityDescription::
ColumnSparsityDescription()
{
    addValue("dense", COLUMN_IS_DENSE, "Column is present in every row");
    addValue("sparse", COLUMN_IS_SPARSE, "Column is present in some rows");
}

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

static CellValue
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

static size_t
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

double coerceTo(const CellValue & val, double *)
{
    return val.coerceToNumber().toDouble();
}

double coerceTo(Date d, double *)
{
    return d.secondsSinceEpoch();
}

template<typename T>
double coerceTo(const T & t, double *,
                typename std::enable_if<std::is_arithmetic<T>::value>::type * = 0)
{
    return t;
}

template<typename T>
double coerceTo(const T & t, double *,
                typename std::enable_if<!std::is_arithmetic<T>::value>::type * = 0)
{
    throw HttpReturnException("can't coerce to numeric type");
}


template<typename Tstorage, typename TOut>
TOut * getValuesT(const void * buf, TOut * out, size_t n)
{
    auto * tbuf = reinterpret_cast<const Tstorage *>(buf);
    for (; n > 0; --n) {
        *out++ = coerceTo(*tbuf++, (TOut *)0);
    }
    return out;
}

template<typename TNumber>
static TNumber *
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

/*****************************************************************************/
/* EXPRESSION VALUE INFO                                                     */
/*****************************************************************************/

ExpressionValueInfo::
~ExpressionValueInfo()
{
}

std::shared_ptr<ExpressionValueInfo>
ExpressionValueInfo::
getCovering(const std::shared_ptr<ExpressionValueInfo> & info1,
            const std::shared_ptr<ExpressionValueInfo> & info2)
{
    // TODO: this is a hack to get going...

    // clang++ 3.6 warning suppression
    auto & dinfo1 = *info1;
    auto & dinfo2 = *info2;

    if (&typeid(dinfo1) == &typeid(dinfo2))
        return info1;

    return std::make_shared<AnyValueInfo>();
}

std::shared_ptr<RowValueInfo>
ExpressionValueInfo::
getMerged(const std::shared_ptr<ExpressionValueInfo> & info1,
          const std::shared_ptr<ExpressionValueInfo> & info2,
          MergeColumnInfo mergeColumnInfo)
{
    auto cols1 = info1->getKnownColumns();
    auto cols2 = info2->getKnownColumns();

    std::unordered_map<ColumnName, std::pair<KnownColumn, KnownColumn> >
        allInfo;
    for (auto & c: cols1)
        allInfo[c.columnName].first = c;
    for (auto & c: cols2)
        allInfo[c.columnName].second = c;
    
    for (auto & c: allInfo) {
        if (c.second.first.valueInfo && c.second.second.valueInfo) {
            c.second.first.sparsity
                = c.second.first.sparsity == COLUMN_IS_SPARSE
                && c.second.second.sparsity == COLUMN_IS_SPARSE
                ? COLUMN_IS_SPARSE : COLUMN_IS_DENSE;
        }
        else if (c.second.first.valueInfo) {
            // Info is already there
        }
        else if (c.second.second.valueInfo) {
            c.second.first = std::move(c.second.second);
        }
        else {
            throw HttpReturnException(500, "Column with no value info");
        }

        if (mergeColumnInfo)
            c.second.first.valueInfo
                = mergeColumnInfo(c.first,
                                  c.second.first.valueInfo,
                                  c.second.second.valueInfo);
    }

    // Now, extract them in order of column name produced by the input
    std::vector<KnownColumn> colsOut;
    for (auto & c: cols1) {
        colsOut.emplace_back(std::move(allInfo[c.columnName].first));
        allInfo.erase(c.columnName);
    }

    // The second ones, which are assumed to be added once the first lot
    // have been dealt with
    for (auto & c: cols2) {
        if (!allInfo.count(c.columnName))
            continue;
        colsOut.emplace_back(std::move(allInfo[c.columnName].first));
    }

    SchemaCompleteness unk1 = info1->getSchemaCompleteness();
    SchemaCompleteness unk2 = info2->getSchemaCompleteness();

    return std::make_shared<RowValueInfo>(std::move(colsOut),
                                          (unk1 == SCHEMA_OPEN || unk2 == SCHEMA_OPEN
                                           ? SCHEMA_OPEN : SCHEMA_CLOSED));
}

std::shared_ptr<RowValueInfo>
ExpressionValueInfo::
getFlattenedInfo() const
{
    throw HttpReturnException(500, "Non-scalar values need to implement getFlattenedInfo()");
}

void
ExpressionValueInfo::
flatten(const ExpressionValue & value,
        const std::function<void (const ColumnName & columnName,
                                  const CellValue & value,
                                  Date timestamp)> & write) const
{
    throw HttpReturnException(500, "Non-scalar values need to implement flatten()");
}

bool
ExpressionValueInfo::
isRow() const
{
    return false;
}

std::shared_ptr<RowValueInfo>
ExpressionValueInfo::
toRow(std::shared_ptr<ExpressionValueInfo> row)
{
    ExcAssert(row);
    auto result = dynamic_pointer_cast<RowValueInfo>(row);
    if (!result)
        throw HttpReturnException(500, "Value is not a row: " + ML::type_name(*row));
    return result;
}

bool
ExpressionValueInfo::
isEmbedding() const
{
    return false;
}

SchemaCompleteness
ExpressionValueInfo::
getSchemaCompleteness() const
{
    throw HttpReturnException(500, "Value description doesn't describe a row: type " + ML::type_name(*this) + " " + jsonEncodeStr(std::shared_ptr<ExpressionValueInfo>(const_cast<ExpressionValueInfo *>(this), [] (ExpressionValueInfo *) {})),
                              "type", ML::type_name(*this));
}

std::vector<KnownColumn>
ExpressionValueInfo::
getKnownColumns() const
{
    throw HttpReturnException(500, "Value description doesn't describe a row",
                              "type", ML::type_name(*this));
}

std::vector<ColumnName>
ExpressionValueInfo::
allColumnNames() const
{
    std::vector<ColumnName> result;
    for (auto & val: getKnownColumns())
        result.emplace_back(std::move(val.columnName));
    return result;
}

std::string
ExpressionValueInfo::
getScalarDescription() const
{
    throw HttpReturnException(500, "Value description doesn't describe a scalar",
                              "type", ML::type_name(*this));
}

std::shared_ptr<ExpressionValueInfo>
ExpressionValueInfo::
findNestedColumn(const ColumnName& columnName) const
{
    return nullptr;
}

std::shared_ptr<ExpressionValueInfo>
ExpressionValueInfo::
getColumn(const Coord & columnName) const
{
    return nullptr;
}

std::vector<ssize_t>
ExpressionValueInfo::
getEmbeddingShape() const
{
    return {};
}

StorageType
ExpressionValueInfo::
getEmbeddingType() const
{
    return ST_ATOM;
}

ExpressionValueInfo::ExtractDoubleEmbeddingFunction
ExpressionValueInfo::
extractDoubleEmbedding(const std::vector<ColumnName> & cols) const
{
    // TODO: specialize this
    return [=] (const ExpressionValue & val)
        {
            return val.getEmbedding(cols.data(), cols.size());
        };
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

struct ExpressionValueInfoPtrDescription
    : public ValueDescriptionT<std::shared_ptr<ExpressionValueInfo> > {

    virtual void parseJsonTyped(std::shared_ptr<ExpressionValueInfo> * val,
                                    JsonParsingContext & context) const
    {
        throw HttpReturnException(400, "ExpressionValueInfoPtrDescription::parseJsonTyped");
    }

    virtual void printJsonTyped(const std::shared_ptr<ExpressionValueInfo> * val,
                                JsonPrintingContext & context) const
    {
        if (!(*val)) {
            context.writeNull();
            return;
        }
        Json::Value out;
        out["type"] = ML::type_name(**val);
        if ((*val)->isScalar()) {
            out["kind"] = "scalar";
            out["scalar"] = (*val)->getScalarDescription();
        }
        else if ((*val)->isEmbedding()) {
            out["kind"] = "embedding";
            out["shape"] = jsonEncode((*val)->getEmbeddingShape());
            out["type"] = jsonEncode((*val)->getEmbeddingType());
        }
        else if ((*val)->isRow()) {
            out["kind"] = "row";
            out["knownColumns"] = jsonEncode((*val)->getKnownColumns());
            out["hasUnknownColumns"] = (*val)->getSchemaCompleteness() == SCHEMA_OPEN;
        }
        context.writeJson(out);
    }

    virtual bool isDefaultTyped(const std::shared_ptr<ExpressionValueInfo> * val) const
    {
        return !*val;
    }
};

DEFINE_VALUE_DESCRIPTION_NS(std::shared_ptr<ExpressionValueInfo>,
                            ExpressionValueInfoPtrDescription);

// BAD SMELL: value description should be able to handle shared ptrs to derived
// types...
struct RowValueInfoPtrDescription
    : public ValueDescriptionT<std::shared_ptr<RowValueInfo> > {

    virtual void parseJsonTyped(std::shared_ptr<RowValueInfo> * val,
                                JsonParsingContext & context) const
    {
        throw HttpReturnException(400, "RowValueInfoPtrDescription::parseJsonTyped");
    }
    
    virtual void printJsonTyped(const std::shared_ptr<RowValueInfo> * val,
                                JsonPrintingContext & context) const
    {
        if (!(*val)) {
            context.writeNull();
            return;
        }
        Json::Value out;
        out["type"] = ML::type_name(**val);
        out["kind"] = "row";
        out["knownColumns"] = jsonEncode((*val)->getKnownColumns());
        out["hasUnknownColumns"] = (*val)->getSchemaCompleteness() == SCHEMA_OPEN;
        context.writeJson(out);
    }
    
    virtual bool isDefaultTyped(const std::shared_ptr<RowValueInfo> * val) const
    {
        return !*val;
    }
};

DEFINE_VALUE_DESCRIPTION_NS(std::shared_ptr<RowValueInfo>,
                            RowValueInfoPtrDescription);


/*****************************************************************************/
/* EXPRESSION VALUE INFO TEMPLATE                                            */
/*****************************************************************************/

#if 0  // GCC 5.3 has trouble with defining this
template<typename Storage>
ExpressionValueInfoT<Storage>::
~ExpressionValueInfoT()
{
}
#endif

template<typename Storage>
size_t
ExpressionValueInfoT<Storage>::
getCellSize() const
{
    return sizeof(Storage);
}

template<typename Storage>
void
ExpressionValueInfoT<Storage>::
initCell(void * data) const
{
    new (data) Storage;
}

template<typename Storage>
void
ExpressionValueInfoT<Storage>::
initCopyFromCell(void * data, void * otherData) const
{
    new (data) Storage(*reinterpret_cast<Storage *>(otherData));
}

template<typename Storage>
void
ExpressionValueInfoT<Storage>::
initMoveFromCell(void * data, void * otherData) const
{
    new (data) Storage(std::move(*reinterpret_cast<Storage *>(otherData)));
}

template<typename Storage>
void
ExpressionValueInfoT<Storage>::
destroyCell(void * data) const
{
    auto val = reinterpret_cast<Storage *>(data);
    val->~Storage();
}

template<typename Storage>
void
ExpressionValueInfoT<Storage>::
moveFromCell(void * data, void * fromData) const
{
    auto val = reinterpret_cast<Storage *>(data);
    auto otherVal = reinterpret_cast<Storage *>(fromData);
    *val = std::move(*otherVal);
}

template<typename Storage>
void
ExpressionValueInfoT<Storage>::
copyFromCell(void * data, const void * fromData) const
{
    auto val = reinterpret_cast<Storage *>(data);
    auto otherVal = reinterpret_cast<const Storage *>(fromData);
    *val = *otherVal;
}


/*****************************************************************************/
/* EXPRESSION VALUE INSTANTIATIONS                                           */
/*****************************************************************************/

EmptyValueInfo::
~EmptyValueInfo()
{
}

size_t
EmptyValueInfo::
getCellSize() const
{
    return 0;
}

void
EmptyValueInfo::
initCell(void * data) const
{
}

void
EmptyValueInfo::
initCopyFromCell(void * data, void * otherData) const
{
}

void
EmptyValueInfo::
initMoveFromCell(void * data, void * otherData) const
{
}

void
EmptyValueInfo::
destroyCell(void * data) const
{
}

void
EmptyValueInfo::
moveFromCell(void * data, void * fromData) const
{
}

void
EmptyValueInfo::
copyFromCell(void * data, const void * fromData) const
{
}

bool
EmptyValueInfo::
isScalar() const
{
    return true;
}


/*****************************************************************************/
/* EMBEDDING VALUE INFO                                                      */
/*****************************************************************************/

EmbeddingValueInfo::
EmbeddingValueInfo(const std::vector<std::shared_ptr<ExpressionValueInfo> > & input)
{
    if (input.empty()) {
        return;
    }

    // First, check to see if our input info is a scalar or a vector
    if (input[0]->isScalar()) {
        
        std::shared_ptr<ExpressionValueInfo> info = input[0];
        for (auto & e: input) {
            if (!e->isScalar()) {
                throw HttpReturnException
                    (400, "Attempt to create scalar embedding element "
                     "from non-scalar",
                     "info", e);
            }
            info = ExpressionValueInfo::getCovering(info, e);
        }

        shape.push_back(input.size());
        return;
    }

    const EmbeddingValueInfo * emb
        = dynamic_cast<const EmbeddingValueInfo *>(input[0].get());

    if (emb) {
        // Recursive embedding
        shape.push_back(input.size());
        for (auto & d: emb->shape)
            shape.push_back(d);
        return;
    }

    // NOTE: this is required to make the usage of EmbeddingValueInfo in
    // JoinElement in the ExecutionPipeline work.  It's not really the
    // right tool for the job there and should be replaced with something
    // else.  At that point, we can make this back into an exception.
    shape = {-1};
    storageType = ST_ATOM;
    return;

    throw HttpReturnException(400, "Cannot determine embedding type from input types",
                              "inputTypes", input);
}

EmbeddingValueInfo::
EmbeddingValueInfo(std::vector<ssize_t> shape, StorageType storageType)
    : shape(std::move(shape)), storageType(storageType)
{
}

bool
EmbeddingValueInfo::
isScalar() const
{
    return false;
}

bool
EmbeddingValueInfo::
isRow() const
{
    return true;
}

bool
EmbeddingValueInfo::
isEmbedding() const
{
    return true;
}

bool
EmbeddingValueInfo::
couldBeScalar() const
{
    return shape.empty();
}

bool
EmbeddingValueInfo::
couldBeRow() const
{
    return true;
}

std::vector<ssize_t>
EmbeddingValueInfo::
getEmbeddingShape() const
{
    return shape;
}

StorageType
EmbeddingValueInfo::
getEmbeddingType() const
{
    return ST_ATOM;
}

size_t
EmbeddingValueInfo::
numDimensions() const
{
    return shape.size();
}

SchemaCompleteness
EmbeddingValueInfo::
getSchemaCompleteness() const
{
    // If we have a -1 index in the shape matrix, we don't know the size of
    // that dimension and thus which columns will be in it.
    for (auto & s: shape)
        if (s < 0)
            return SCHEMA_OPEN;
    return SCHEMA_CLOSED;
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

std::vector<KnownColumn>
EmbeddingValueInfo::
getKnownColumns() const
{
    std::vector<KnownColumn> result;
    
    // If we have a -1 index in the shape matrix, we don't know the size of
    // that dimension and thus how many values will be in it.
    for (auto & s: shape)
        if (s < 0)
            return result;

    auto valueInfo = getValueInfoForStorage(ST_ATOM);

    std::function<void (ColumnName prefix, int dim)> doDim
        = [&] (ColumnName prefix, int dim)
        {
            if (dim == shape.size()) {
                result.emplace_back(std::move(prefix), valueInfo,
                                    COLUMN_IS_DENSE, result.size());
                return;
            }
            
            for (size_t i = 0;  i < shape[dim];  ++i) {
                doDim(prefix + i, dim + 1);
            }
        };

    doDim(ColumnName(), 0);

    return result;
}

std::vector<ColumnName>
EmbeddingValueInfo::
allColumnNames() const
{
    std::vector<ColumnName> result;
    
    // If we have a -1 index in the shape matrix, we don't know the size of
    // that dimension and thus how many values will be in it.
    for (auto & s: shape)
        if (s < 0)
            return result;

    std::function<void (ColumnName prefix, int dim)> doDim
        = [&] (ColumnName prefix, int dim)
        {
            if (dim == shape.size()) {
                result.emplace_back(std::move(prefix));
                return;
            }
                                    
            for (size_t i = 0;  i < shape[dim];  ++i) {
                doDim(prefix + i, dim + 1);
            }
        };

    doDim(ColumnName(), 0);

    return result;
}

std::shared_ptr<RowValueInfo>
EmbeddingValueInfo::
getFlattenedInfo() const
{
    throw HttpReturnException(400, "EmbeddingValueInfo::getFlattenedInfo()");
}

void
EmbeddingValueInfo::
flatten(const ExpressionValue & value,
        const std::function<void (const ColumnName & columnName,
                                  const CellValue & value,
                                  Date timestamp)> & write) const
{
    throw HttpReturnException(400, "EmbeddingValueInfo::flatten()");
}

/*****************************************************************************/
/* ANY VALUE INFO                                                            */
/*****************************************************************************/

AnyValueInfo::
AnyValueInfo()
{
}

bool
AnyValueInfo::
isScalar() const
{
    return false;
}

std::shared_ptr<RowValueInfo>
AnyValueInfo::
getFlattenedInfo() const
{
    throw HttpReturnException(400, "AnyValueInfo::getFlattenedInfo()");
}

void
AnyValueInfo::
flatten(const ExpressionValue & value,
        const std::function<void (const ColumnName & columnName,
                                  const CellValue & value,
                                  Date timestamp)> & write) const
{
    throw HttpReturnException(400, "AnyValueInfo::flatten()");
}

SchemaCompleteness
AnyValueInfo::
getSchemaCompleteness() const
{
    return SCHEMA_OPEN;
}

std::vector<KnownColumn>
AnyValueInfo::
getKnownColumns() const
{
    return {};
}


/*****************************************************************************/
/* ROW VALUE INFO                                                            */
/*****************************************************************************/

RowValueInfo::
RowValueInfo(const std::vector<KnownColumn> & columns,
             SchemaCompleteness completeness)
    : columns(columns),
      completeness(completeness)
{
}

bool
RowValueInfo::
isScalar() const
{
    return false;
}

std::shared_ptr<RowValueInfo>
RowValueInfo::
getFlattenedInfo() const
{
    throw HttpReturnException(400, "RowValueInfo::getFlattenedInfo()");
}

void
RowValueInfo::
flatten(const ExpressionValue & value,
        const std::function<void (const ColumnName & columnName,
                                  const CellValue & value,
                                  Date timestamp)> & write) const
{
    throw HttpReturnException(400, "RowValueInfo::flatten()");
}

std::vector<KnownColumn>
RowValueInfo::
getKnownColumns() const
{
    return columns;
}

SchemaCompleteness
RowValueInfo::
getSchemaCompleteness() const
{
    return completeness;
}

std::shared_ptr<ExpressionValueInfo>
RowValueInfo::
findNestedColumn(const ColumnName& columnName) const
{  
    for (auto& col : columns) {
        if (col.columnName == columnName) {
            // Found directly
            return col.valueInfo;
        }
        else if (columnName.startsWith(col.columnName)) {
            // Nested; look inside the sub-info
            ColumnName tail = columnName.removePrefix(col.columnName.size());
            auto res = col.valueInfo->findNestedColumn(tail);
            if (res)
                return res;
        }
    }
    
    if (completeness == SCHEMA_CLOSED)
        return nullptr;

    return std::make_shared<AnyValueInfo>();
}            

std::shared_ptr<ExpressionValueInfo>
RowValueInfo::
getColumn(const Coord & columnName) const
{
    throw HttpReturnException(600, "RowValueInfo::getColumn()");
#if 0
    for (auto& col : columns) {
        if (col.columnName == columnName) {
            // Found directly
            return col.valueInfo;
        }
        else if (col.columnName.startsWith(columnName)) {
            // Nested; look inside the sub-info
            ColumnName tail = columnName.removePrefix(col.columnName.size());
            return col.valueInfo->findNestedColumn(tail);
        }
    }
    
    return nullptr;  // not found
#endif
}


/*****************************************************************************/
/* EXPRESSION VALUE                                                          */
/*****************************************************************************/

/// This is how we store a structure with a single value for each
/// element and an external set of column names.  There is only
/// one timestamp for the whole thing.
struct ExpressionValue::Flattened {
    std::shared_ptr<const std::vector<ColumnName> > columnNames;
    std::vector<CellValue> values;

    size_t length() const
    {
        return values.size();
    }

    const CellValue & value(int i) const
    {
        return values.at(i);
    }

    CellValue moveValue(int i)
    {
        return std::move(values.at(i));
    }

    ColumnName columnName(int i) const
    {
        if (columnNames)
            return columnNames->at(i);
        // TODO: static list of the first 1,000 column names to avoid allocs
        else return ColumnName(i);
    }
};

/// This is how we store a embedding, which is a dense array of a
/// uniform data type.
struct ExpressionValue::Embedding {
    std::shared_ptr<const void> data_;
    StorageType storageType_;
    std::vector<size_t> dims_;
    std::shared_ptr<const EmbeddingMetadata> metadata_;

    size_t length() const
    {
        size_t result = 1;
        for (auto & d: dims_)
            result *= d;
        return result;
    }

    CellValue getValue(size_t n) const
    {
        return MLDB::getValue(data_.get(), storageType_, n);
    }

    const ExpressionValue *
    tryGetNestedColumn(const Coords & column, ExpressionValue & storage,
                       Date ts) const
    {
        if (column.size() == dims_.size()) {
            // we're getting an atom.  Look up the element we need
            size_t offset = 0;
            size_t stride = length();
            for (size_t i = 0;  i < column.size();  ++i) {
                stride /= dims_[i];
                const Coord & coord = column[i];
                if (!coord.isIndex())
                    return nullptr; // not an index
                size_t index = coord.toIndex();
                if (index >= dims_[i])
                    return nullptr; // out of range index
                offset += stride * index;
            }

            return &(storage = ExpressionValue(getValue(offset), ts));
        }
        else if (column.size() > dims_.size()) {
            return nullptr;  // too many indexes
        }
        else {
            // we're getting a sub-embedding
            // We need to calculate:
            // a) a new set of dims
            // b) the offset of the start of our value
            // c) new dimensions for the value
            size_t offset = 0;
            size_t stride = length();
            for (size_t i = 0;  i < column.size();  ++i) {
                stride /= dims_[i];
                const Coord & coord = column[i];
                if (!coord.isIndex())
                    return nullptr;
                size_t index = coord.toIndex();
                offset += stride * index;
            }

            std::vector<size_t> newDims;
            for (size_t i = column.size();  i < dims_.size();  ++i)
                newDims.emplace_back(dims_[i]);
            
            const char * p = reinterpret_cast<const char *>(data_.get());
            size_t offsetBytes = getCellSizeInBytes(storageType_) * offset;
            const void * start = p + offsetBytes;
            return &(storage = ExpressionValue::embedding
                     (ts,
                      std::shared_ptr<const void>(data_, start),
                      storageType_, newDims));
        }
    }

    bool forEachValue(std::function<bool (const std::vector<int> & indexes,
                                          CellValue & val)> onVal) const
    {
        vector<int> indexes(dims_.size());
        size_t n = 0;

        std::function<bool (int level)> runLevel = [&] (int i)
            {
                if (i == dims_.size()) {
                    CellValue val = getValue(n++);
                    return onVal(indexes, val);
                }
                else {
                    for (indexes[i] = 0;  indexes[i] < dims_[i];
                         ++indexes[i]) {
                        if (!runLevel(i + 1))
                            return false;
                    }
                    return true;
                }
            };

        return runLevel(0);
    }

    bool forEachAtom(std::function<bool (ColumnName & col,
                                         CellValue & val)> onColumn) const
    {
        auto onValue = [&] (const vector<int> & indexes, CellValue & val)
            {
                Coords c;
                for (auto & i: indexes) {
                    c = c + i;
                }

                return onColumn(c, val);
            };

        return forEachValue(onValue);
    }

    bool forEachColumn(std::function<bool (Coord & col,
                                           ExpressionValue & val)> onColumn,
                       Date ts) const
    {
        // We destructure into shape[0] elements, each of which is
        // either a sub-embedding or an element
        if (dims_.empty()) {
            // Nothing stored there at all
            return true;
        }

        if (dims_.size() == 1) {
            // Each value is a simple value
            for (size_t i = 0;  i < dims_[0];  ++i) {
                ExpressionValue val(getValue(i), ts);
                Coord col(i);
                if (!onColumn(col, val))
                    return false;
            }

            return true;
        }
        else {
            // Each value is a sub-embedding
            std::vector<size_t> newDims;
            newDims.reserve(dims_.size() - 1);
            size_t stride = 1;
            for (size_t i = 1;  i < dims_.size();  ++i) {
                stride *= dims_[i];
                newDims.emplace_back(dims_[i]);
            }

            size_t strideBytes = getCellSizeInBytes(storageType_) * stride;
            const char * p = reinterpret_cast<const char *>(data_.get());

            for (size_t i = 0;  i < dims_[0];  ++i) {
                const void * start = p + i * strideBytes;
                ExpressionValue val
                    = embedding(ts, std::shared_ptr<const void>(data_, start),
                                storageType_, newDims);
                Coord col(i);
                if (!onColumn(col, val))
                    return false;
            }

            return true;
        }
    }

    /** Write the value, including metadata and encoding types, to JSON for
        more efficient deserialization.
    */
    void writeJson(JsonPrintingContext & context) const
    {
        context.startObject();
        context.startMember("shape");
        context.writeJson(jsonEncode(dims_));
        context.startMember("type");
        context.writeString(jsonEncode(storageType_).asString());
        context.startMember("val");

        extractJson(context);

        context.endObject();
    }

    /** Print only the value, not the metadata, as JSON. */
    void extractJson(JsonPrintingContext & context) const
    {
        vector<int> indexes(dims_.size());
        size_t n = 0;

        std::function<void (int level)> runLevel = [&] (int i)
            {
                static auto cellDesc = getDefaultDescriptionShared((CellValue *)0);
                if (i == dims_.size()) {
                    CellValue val(getValue(n++));
                    cellDesc->printJsonTyped(&val, context);
                }
                else {
                    context.startArray(dims_[i]);
                    for (indexes[i] = 0;  indexes[i] < dims_[i];
                         ++indexes[i]) {
                        context.newArrayElement();
                        runLevel(i + 1);
                    }
                    context.endArray();
                }
            };
        
        runLevel(0);
    }

    std::shared_ptr<EmbeddingValueInfo>
    getSpecializedValueInfo() const
    {
        return std::make_shared<EmbeddingValueInfo>
            (vector<ssize_t>(dims_.begin(), dims_.end()), storageType_);
    }
};

struct ExpressionValue::Superposition {
    std::vector<ExpressionValue> values;

    ExpressionValue latest() const
    {
        throw HttpReturnException(600, __PRETTY_FUNCTION__);
    }

    ExpressionValue earliest() const
    {
        throw HttpReturnException(600, __PRETTY_FUNCTION__);
    }

    size_t length() const
    {
        throw HttpReturnException(600, __PRETTY_FUNCTION__);
    }

    CellValue getValue(size_t n) const
    {
        throw HttpReturnException(600, __PRETTY_FUNCTION__);
    }

    const ExpressionValue *
    tryGetNestedColumn(const Coords & column, ExpressionValue & storage,
                       Date ts) const
    {
        throw HttpReturnException(600, __PRETTY_FUNCTION__);
    }

    bool forEachValue(std::function<bool (const std::vector<int> & indexes,
                                          CellValue & val)> onVal) const
    {
        throw HttpReturnException(600, __PRETTY_FUNCTION__);
    }

    bool forEachAtom(std::function<bool (ColumnName & col,
                                         CellValue & val)> onColumn) const
    {
        throw HttpReturnException(600, __PRETTY_FUNCTION__);
    }

    bool forEachColumn(std::function<bool (Coord & col,
                                           ExpressionValue & val)> onColumn,
                       Date ts) const
    {
        throw HttpReturnException(600, __PRETTY_FUNCTION__);
    }

    /** Write the value, including metadata and encoding types, to JSON for
        more efficient deserialization.
    */
    void writeJson(JsonPrintingContext & context) const
    {
        throw HttpReturnException(600, __PRETTY_FUNCTION__);
    }

    /** Print only the value, not the metadata, as JSON. */
    void extractJson(JsonPrintingContext & context) const
    {
        throw HttpReturnException(600, __PRETTY_FUNCTION__);
    }

    std::shared_ptr<ExpressionValueInfo>
    getSpecializedValueInfo() const
    {
        throw HttpReturnException(600, __PRETTY_FUNCTION__);
    }
};


static_assert(sizeof(CellValue) <= 24, "CellValue is too big to fit");
static_assert(sizeof(ExpressionValue::Structured) <= 24, "Structured is too big to fit");

ExpressionValue::
ExpressionValue()
    : type_(Type::NONE)
{
}

ExpressionValue::
ExpressionValue(std::nullptr_t, Date ts)
    : type_(Type::NONE), ts_(ts)
{
}

ExpressionValue
ExpressionValue::
null(Date ts)
{
    return ExpressionValue(nullptr, ts);
}

ExpressionValue::
ExpressionValue(Date val, Date ts)
    : type_(Type::NONE)
{
    initAtom(val, ts);
}

ExpressionValue::
ExpressionValue(const std::string & asciiStringValue, Date ts)
    : type_(Type::NONE)
{
    initAtom(asciiStringValue, ts);
}

ExpressionValue::
ExpressionValue(const char * asciiStringValue, Date ts)
    : type_(Type::NONE)
{
    initAtom(asciiStringValue, ts);
}

ExpressionValue::
ExpressionValue(const Utf8String & unicodeStringValue, Date ts)
    : type_(Type::NONE)
{
    initAtom(unicodeStringValue, ts);
}

ExpressionValue::
ExpressionValue(const std::basic_string<char32_t> & utf32StringValue, Date ts)
    : type_(Type::NONE)
{
    initAtom(Utf8String(utf32StringValue), ts);
}

ExpressionValue::
ExpressionValue(CellValue atom, Date ts) noexcept
    : type_(Type::NONE)
{
    initAtom(std::move(atom), ts);
}

ExpressionValue::
ExpressionValue(const Json::Value & val, Date timestamp)
    : type_(Type::NONE)
{
    if (!val.isObject() && !val.isArray()) {
        initAtom(jsonDecode<CellValue>(val), timestamp);
        return;
    }

    ts_ = timestamp;

    StructValue row;

    if (val.isObject()) {
        for (auto it = val.begin(), end = val.end();  it != end;  ++it) {
            Coord columnName(it.memberName());
            row.emplace_back(columnName, ExpressionValue(*it, timestamp));
        }
    }
    else if (val.isArray()) {
        for (unsigned i = 0;  i < val.size();  ++i) {
            Coord columnName(i);
            row.emplace_back(columnName, ExpressionValue(val[i], timestamp));
        }
    }

    initStructured(std::move(row));
}

static const auto cellValueDesc = getDefaultDescriptionShared((CellValue *)0);

CellValue expectAtom(JsonParsingContext & context)
{
    CellValue val;
    cellValueDesc->parseJson(&val, context);
    return val;
}

ExpressionValue
ExpressionValue::
parseJson(JsonParsingContext & context,
          Date timestamp,
          JsonArrayHandling arrays)
{
    if (context.isObject()) {

        std::vector<std::tuple<Coord, ExpressionValue> > out;
        out.reserve(16);  // TODO: context may know object size

        auto onObjectField = [&] ()
            {
                const char * fieldName = context.fieldNamePtr();
                out.emplace_back
                    (Coord(fieldName, strlen(fieldName)),
                 parseJson(context, timestamp, arrays));
            };
        context.forEachMember(onObjectField);

        return std::move(out);
    }
    else if (context.isArray()) {
        std::vector<std::tuple<Coord, ExpressionValue> > out;
        out.reserve(16);  // TODO: context may know array length

        bool hasNonAtom = false;
        bool hasNonObject = false;

        auto onArrayElement = [&] ()
            {
                if (!context.isObject())
                    hasNonObject = true;
                out.emplace_back(context.fieldNumber(),
                                 parseJson(context, timestamp, arrays));
                
                if (!std::get<1>(out.back()).isAtom())
                    hasNonAtom = true;
            };
        
        context.forEachElement(onArrayElement);

        if (arrays == ENCODE_ARRAYS && !hasNonAtom) {
            // One-hot encode them
            for (auto & v: out) {
                Coord & columnName = std::get<0>(v);
                ExpressionValue & columnValue = std::get<1>(v);
                
                columnName = Coord(columnValue.toUtf8String());
                columnValue = ExpressionValue(1, timestamp);
            }
        }
        else if (arrays == ENCODE_ARRAYS && !hasNonObject) {
            // JSON encode them
            for (auto & v: out) {
                ExpressionValue & columnValue = std::get<1>(v);
                std::string str;
                StringJsonPrintingContext context(str);
                columnValue.extractJson(context);
                columnValue = ExpressionValue(str, timestamp);
            }
        }

        return std::move(out);
    }
    else {
        // It's an atomic cell value; parse the atom
        return ExpressionValue(expectAtom(context), timestamp);
    }
}

ExpressionValue::
ExpressionValue(RowValue row) noexcept
: type_(Type::NONE)
{
    // Convert to a structured representation
    // this means that
    // x.y.z = 1
    // x.y.q = 2
    //
    // becomes
    //
    // x: { y: { z: 1, q: 2 } }
    //
    // in other words, we reconstitute a flattened representation into the
    // structured object that it represents.

    // First, we scan to see what keys are available at this level and other
    // statistics to figure out if we can do some smart optimizations.
    bool isSorted = true;
    bool oneTimestamp = true;
    bool noRepeatedKeys = true;
    size_t maxPathLength = row.empty() ? 0 : std::get<0>(row[0]).size();

    for (size_t i = 1;  i < row.size();  ++i) {
        if (isSorted && std::get<0>(row[i - 1]) > std::get<0>(row[i]))
            isSorted = false;
        if (noRepeatedKeys && std::get<0>(row[i - 1]) == std::get<0>(row[i]))
            noRepeatedKeys = false;
        if (oneTimestamp && std::get<2>(row[i - 1]) != std::get<2>(row[i]))
            oneTimestamp = false;
        maxPathLength = std::max<size_t>(maxPathLength, std::get<0>(row[i]).size());
    }

    // If we have a single timestamp, we can use the more efficient
    // structured representation.
    
    // ...

    if (!isSorted)
        std::sort(row.begin(), row.end());

    // Structure a flattened representation.  The range between first
    // and last must all start with the same prefix, of length at least
    // level.
    std::function<Structured (RowValue::iterator first,
                              RowValue::iterator last,
                              size_t level)>
        doLevel = [&] (RowValue::iterator first,
                       RowValue::iterator last,
                       size_t level)
        {
            Structured rowOut;
            
            // Count how many unique keys there are?
            size_t numUnique = 0;

            for (auto it = first;  it != last;  ++it) {
                ExcAssert(std::get<0>(*it).size() > level);
                
                if (std::get<0>(*it).size() == level + 1) {
                    // This is a final value, and so gets its own value
                    ++numUnique;
                    continue;
                }
                // Is the element at this level different from the last
                // one?  If so, we have a new unique element.
                if (it == first
                    || (std::get<0>(*it).at(level)
                        != std::get<0>(*(std::prev(it))).at(level)))
                    ++numUnique;
            }
            
            rowOut.reserve(numUnique);

            for (auto it = first;  it != last;  /* no inc */) {
                if (std::get<0>(*it).size() == level + 1) {
                    // This is a final value.

                    rowOut.emplace_back(std::get<0>(*it).back(),
                                        ExpressionValue(std::move(std::get<1>(*it)),
                                                        std::get<2>(*it)));
                    ++it;
                    continue;
                }

                auto it2 = std::next(it);
                while (it2 != last
                       && std::get<0>(*it2).at(level)
                       == std::get<0>(*it).at(level))
                    ++it2;

                Structured newValue = doLevel(it, it2, level + 1);
                rowOut.emplace_back(std::move(std::get<0>(*it).at(level)),
                                    std::move(newValue));

                it = it2;
            }

            return rowOut;
        };

    initStructured(doLevel(row.begin(), row.end(), 0 /* level */));
}

ExpressionValue::
~ExpressionValue()
{
    typedef std::shared_ptr<const Structured> StructuredRepr;
    typedef std::shared_ptr<const Flattened> FlattenedRepr;
    typedef std::shared_ptr<const Embedding> EmbeddingRepr;
    typedef std::shared_ptr<const Superposition> SuperpositionRepr;

    switch (type_) {
    case Type::NONE: return;
    case Type::ATOM: cell_.~CellValue();  return;
    case Type::STRUCTURED:  structured_.~StructuredRepr();  return;
    case Type::EMBEDDING: embedding_.~EmbeddingRepr();  return;
    case Type::SUPERPOSITION: superposition_.~SuperpositionRepr();  return;
    }
    throw HttpReturnException(400, "Unknown expression value type");
}

ExpressionValue::
ExpressionValue(const ExpressionValue & other)
    : type_(Type::NONE)
{
    switch (other.type_) {
    case Type::NONE: ts_ = other.ts_;  return;
    case Type::ATOM: initAtom(other.cell_, other.ts_);  return;
    case Type::STRUCTURED:  initStructured(other.structured_);  return;
    case Type::EMBEDDING: {
        ts_ = other.ts_;
        new (storage_) std::shared_ptr<const Embedding>(other.embedding_);
        type_ = Type::EMBEDDING;
        return;
    }
    case Type::SUPERPOSITION: {
        ts_ = other.ts_;
        new (storage_) std::shared_ptr<const Superposition>(other.superposition_);
        type_ = Type::SUPERPOSITION;
        return;
    }
    }
    throw HttpReturnException(400, "Unknown expression value type");
}

ExpressionValue::
ExpressionValue(ExpressionValue && other) noexcept
    : type_(Type::NONE)
{
    ts_ = other.ts_;
    type_ = other.type_;

    switch (other.type_) {
    case Type::NONE: return;
    case Type::ATOM: new (storage_) CellValue(std::move(other.cell_));  return;
    case Type::STRUCTURED:  new (storage_) std::shared_ptr<const Structured>(std::move(other.structured_));  return;
    case Type::EMBEDDING: new (storage_) std::shared_ptr<const Embedding>(std::move(other.embedding_));  return;
    case Type::SUPERPOSITION: new (storage_) std::shared_ptr<const Superposition>(std::move(other.superposition_));  return;
    }
    throw HttpReturnException(400, "Unknown expression value type");
}

ExpressionValue::
ExpressionValue(std::vector<std::tuple<Coord, ExpressionValue> > vals) noexcept
    : type_(Type::NONE)
{
    initStructured(std::move(vals));
}

ExpressionValue &
ExpressionValue::
operator = (const ExpressionValue & other)
{
    ExpressionValue newMe(other);
    swap(newMe);
    return *this;
}

ExpressionValue &
ExpressionValue::
operator = (ExpressionValue && other) noexcept
{
    ExpressionValue newMe(std::move(other));
    swap(newMe);
    return *this;
}

void
ExpressionValue::
swap(ExpressionValue & other) noexcept
{
    // Dodgy as hell.  But none of them have self referential pointers, and so it
    // works and with no possibility of an exception.
    std::swap(type_,    other.type_);
    std::swap(storage_[0], other.storage_[0]);
    std::swap(storage_[1], other.storage_[1]);
    std::swap(ts_, other.ts_);
}

ExpressionValue::
ExpressionValue(std::vector<CellValue> values,
                std::shared_ptr<const std::vector<ColumnName> > cols,
                Date ts)
    : type_(Type::NONE), ts_(ts)
{
    ExcAssertEqual(values.size(), (*cols).size());

    RowValue row;
    row.reserve(cols->size());
    for (size_t i = 0;  i < cols->size();  ++i) {
        row.emplace_back(cols->at(i), std::move(values[i]), ts);
    }

    ExpressionValue v(std::move(row));
    v.swap(*this);
}

ExpressionValue::
ExpressionValue(const std::vector<double> & values,
                std::shared_ptr<const std::vector<ColumnName> > cols,
                Date ts)
    : type_(Type::NONE), ts_(ts)
{
    ExcAssertEqual(values.size(), (*cols).size());

    ExcAssertEqual(values.size(), (*cols).size());

    RowValue row;
    row.reserve(cols->size());
    for (size_t i = 0;  i < cols->size();  ++i) {
        row.emplace_back(cols->at(i), values[i], ts);
    }

    ExpressionValue v(std::move(row));
    v.swap(*this);
}

ExpressionValue::
ExpressionValue(std::vector<CellValue> values,
                Date ts,
                std::vector<size_t> shape)
    : type_(Type::NONE), ts_(ts)
{
    std::shared_ptr<CellValue> vals(new CellValue[values.size()],
                                    [] (CellValue * p) { delete[] p; });
    std::copy(std::make_move_iterator(values.begin()),
              std::make_move_iterator(values.end()),
              vals.get());
    std::shared_ptr<Embedding> content(new Embedding());
    content->data_ = std::move(vals);
    content->storageType_ = ST_ATOM;
    content->dims_ = std::move(shape);
    if (content->dims_.empty())
        content->dims_ = { values.size() };
    
    new (storage_) std::shared_ptr<const Embedding>(std::move(content));
    type_ = Type::EMBEDDING;
}

ExpressionValue::
ExpressionValue(std::vector<float> values, Date ts,
                std::vector<size_t> shape)
    : type_(Type::NONE), ts_(ts)
{
    std::shared_ptr<float> vals(new float[values.size()],
                                    [] (float * p) { delete[] p; });
    std::copy(std::make_move_iterator(values.begin()),
              std::make_move_iterator(values.end()),
              vals.get());
    std::shared_ptr<Embedding> content(new Embedding());
    content->data_ = std::move(vals);
    content->storageType_ = ST_FLOAT32;
    content->dims_ = std::move(shape);
    if (content->dims_.empty())
        content->dims_ = { values.size() };
    
    new (storage_) std::shared_ptr<const Embedding>(std::move(content));
    type_ = Type::EMBEDDING;
}

ExpressionValue::
ExpressionValue(std::vector<double> values, Date ts,
                std::vector<size_t> shape)
    : type_(Type::NONE), ts_(ts)
{
    std::shared_ptr<double> vals(new double[values.size()],
                                    [] (double * p) { delete[] p; });
    std::copy(std::make_move_iterator(values.begin()),
              std::make_move_iterator(values.end()),
              vals.get());
    std::shared_ptr<Embedding> content(new Embedding());
    content->data_ = std::move(vals);
    content->storageType_ = ST_FLOAT64;
    content->dims_ = std::move(shape);
    if (content->dims_.empty())
        content->dims_ = { values.size() };
    
    new (storage_) std::shared_ptr<const Embedding>(std::move(content));
    type_ = Type::EMBEDDING;
}

ExpressionValue
ExpressionValue::
embedding(Date ts,
       std::shared_ptr<const void> data,
       StorageType storageType,
       std::vector<size_t> dims,
       std::shared_ptr<const EmbeddingMetadata> md)
{
    auto embeddingData = std::make_shared<Embedding>();
    embeddingData->data_ = std::move(data);
    embeddingData->storageType_ = storageType;
    embeddingData->dims_ = std::move(dims);
    embeddingData->metadata_ = std::move(md);

    ExpressionValue result;
    result.ts_ = ts;
    result.type_ = Type::EMBEDDING;
    new (result.storage_) std::shared_ptr<const Embedding>(std::move(embeddingData));

    return result;
}

ExpressionValue
ExpressionValue::
superpose(std::vector<ExpressionValue> vals)
{
    if (vals.empty()) {
        return ExpressionValue();
    }
    else if (vals.size() == 1) {
        return std::move(vals[0]);
    }
    else {
        StructValue result;
        result.reserve(vals.size());
        for (ExpressionValue & v: vals) {
            if (v.empty() || v.isAtom()) {
                result.emplace_back(Coord(), std::move(v));
            }
            else {
                auto onColumn = [&] (Coord & columnName,
                                     ExpressionValue & val)
                    {
                        result.emplace_back(std::move(columnName),
                                            std::move(val));
                        return true;
                    };

                v.forEachColumnDestructive(onColumn);
            }
        }
        std::sort(result.begin(), result.end());
        return std::move(result);
    }
}

ExpressionValue
ExpressionValue::
fromInterval(uint16_t months, uint16_t days, float seconds, Date ts)
{
    CellValue cellValue = CellValue::fromMonthDaySecond(months, days ,seconds);
    return ExpressionValue(cellValue, ts);
}

double
ExpressionValue::
toDouble() const
{
    assertType(Type::ATOM, "double");
    return cell_.toDouble();
}

int64_t
ExpressionValue::
toInt() const
{
    assertType(Type::ATOM, "integer");
    return cell_.toInt();
}
    
bool
ExpressionValue::
asBool() const
{
    if (type_ == Type::NONE)
        return false;
    
    assertType(Type::ATOM, "boolean");

    return cell_.asBool();
}

bool
ExpressionValue::
isTrue() const
{
    switch (type_) {
    case Type::NONE:
        return false;
    case Type::ATOM:
        return cell_.isTrue();
    case Type::STRUCTURED:
        return !structured_->empty();
    case Type::EMBEDDING:
        return embedding_->length();
    case Type::SUPERPOSITION:
        return superposition_->latest().isTrue();
    }

    throw HttpReturnException(500, "Unknown expression value type");
}

bool
ExpressionValue::
isFalse() const
{
    switch (type_) {
    case Type::NONE:
        return false;
    case Type::ATOM:
        return cell_.isFalse();
    case Type::STRUCTURED:
        return structured_->empty();
    case Type::EMBEDDING:
        return embedding_->length();
    case Type::SUPERPOSITION:
        return superposition_->latest().isFalse();
    }

    throw HttpReturnException(500, "Unknown expression value type");
}

bool
ExpressionValue::
empty() const
{
    if (type_ == Type::NONE)
        return true;
    if (type_ == Type::ATOM)
        return cell_.empty();
    return false;
}

bool
ExpressionValue::
isString() const
{
    if (type_ != Type::ATOM)
        return false;
    return cell_.isString();
}

bool
ExpressionValue::
isUtf8String() const
{
    if (type_ != Type::ATOM)
        return false;
    return cell_.isUtf8String();
}

bool
ExpressionValue::
isAsciiString() const
{
    if (type_ != Type::ATOM)
        return false;
    return cell_.isAsciiString();
}

bool
ExpressionValue::
isInteger() const
{
    if (type_ != Type::ATOM)
        return false;
    return cell_.isInteger();
}

bool
ExpressionValue::
isNumber() const
{
    if (type_ != Type::ATOM)
        return false;
    return cell_.isNumber();
}

bool
ExpressionValue::
isTimestamp() const
{
    if (type_ != Type::ATOM)
        return false;
    return cell_.isTimestamp();
}

bool
ExpressionValue::
isTimeinterval() const
{
    if (type_ != Type::ATOM)
        return false;
    return getAtom().isTimeinterval();
}

#if 0
bool
ExpressionValue::
isObject() const
{
    return type_ == Type::STRUCTURED;
}
#endif

bool
ExpressionValue::
isArray() const
{
    return type_ == Type::STRUCTURED;
}

bool
ExpressionValue::
isAtom() const
{
    return type_ == Type::ATOM || type_ == Type::NONE;
}

bool
ExpressionValue::
isRow() const
{
    return type_ == Type::STRUCTURED || type_ == Type::EMBEDDING;
}

bool
ExpressionValue::
isSuperposition() const
{
    //return type_ == Type::SUPERPOSITION;
    return type_ == Type::STRUCTURED
        && structured_->size() > 0
        && std::get<0>(structured_->at(0)).empty();
}

bool
ExpressionValue::
isEmbedding() const
{
    return type_ == Type::EMBEDDING;
}

std::string
ExpressionValue::
toString() const
{
    assertType(Type::ATOM, "string");
    return cell_.toString();
}

Utf8String
ExpressionValue::
toUtf8String() const
{
    assertType(Type::ATOM, "Utf8 string");
    return cell_.toUtf8String();
}

string
ExpressionValue::
getTypeAsString() const
{
    return print(type_);
}

std::basic_string<char32_t>
ExpressionValue::
toWideString() const
{
    assertType(Type::ATOM, "wide string");
    return cell_.toWideString();
}

Date
ExpressionValue::
getMinTimestamp() const
{
    if (type_ == Type::NONE || type_ == Type::ATOM)
        return ts_;

    Date result = Date::positiveInfinity();

    auto onSubex = [&] (const Coords & columnName,
                        const ExpressionValue & val)
        {
            result.setMin(val.getMinTimestamp());
            return true;
        };

    forEachColumn(onSubex);

    return result;
}

Date
ExpressionValue::
getMaxTimestamp() const
{
    if (type_ == Type::NONE || type_ == Type::ATOM)
        return ts_;

    Date result = Date::negativeInfinity();

    auto onSubex = [&] (const Coords & columnName,
                        const ExpressionValue & val)
        {
            result.setMax(val.getMaxTimestamp());
            return true;
        };
    
    forEachColumn(onSubex);

    return result;
}

bool 
ExpressionValue::
isEarlier(const Date& compareTimeStamp, const ExpressionValue& compareValue) const
{
    Date ts = getEffectiveTimestamp();
    return (ts < compareTimeStamp) || (ts == compareTimeStamp && *this < compareValue);
}

bool 
ExpressionValue::
isLater(const Date& compareTimeStamp, const ExpressionValue& compareValue) const
{
    Date ts = getEffectiveTimestamp();
    return (ts > compareTimeStamp) || (ts == compareTimeStamp && *this < compareValue);
}

namespace {

struct FilterAccumulator {
    FilterAccumulator(const VariableFilter & filter)
        : filter(filter)
    {
    }

    VariableFilter filter;

    bool operator () (ExpressionValue && val)
    {
        return accum(std::move(val));
    }

    bool operator () (const ExpressionValue & val)
    {
        return accum(val);
    }

    template<typename ExpressionValueT>
    bool accum(ExpressionValueT && val)
    {
        if (val.isRow()) {
            auto onColumn = [&] (const Coord & columnName,
                                 const ExpressionValue & expr)
                {
                    if (columnName.empty()) {
                        // This is a superposition, and so we need to go
                        // down a level
                        accum(expr);
                    }
                    else {
                        foundRows.emplace_back(columnName, expr);
                    }
                    return true;
                };

            val.forEachColumn(onColumn);
            return true;
        }

        switch (filter) {
        case GET_ANY_ONE:
            if (foundAtoms.empty())
                foundAtoms.emplace_back(std::move(val));
            return false;  // finished
        
        case GET_EARLIEST:
            if (foundAtoms.empty()
                || foundAtoms[0].getEffectiveTimestamp() > val.getEffectiveTimestamp()
                || (foundAtoms[0].getEffectiveTimestamp() == val.getEffectiveTimestamp()
                    && val < foundAtoms[0])) {
                if (foundAtoms.empty())
                    foundAtoms.emplace_back(std::move(val));
                else foundAtoms[0] = std::move(val);
            }
            return true;  // need to continue

        case GET_LATEST:
            if (foundAtoms.empty()
                || foundAtoms[0].getEffectiveTimestamp() < val.getEffectiveTimestamp()
                || (foundAtoms[0].getEffectiveTimestamp() == val.getEffectiveTimestamp()
                    && val < foundAtoms[0])) {

                if (foundAtoms.empty())
                    foundAtoms.emplace_back(std::move(val));
                else foundAtoms[0] = std::move(val);
            }
            return true;  // need to continue

        case GET_ALL: {
            foundAtoms.emplace_back(std::move(val));
            return true;  // need to continue

        default:
            throw HttpReturnException(500, "Unknown variable filter");
        }
        }
    }

    bool empty() const { return foundAtoms.empty() && foundRows.empty(); }

    const ExpressionValue * extract(ExpressionValue & storage)
    {
        //cerr << "got " << foundRows.size() << " rows and "
        //     << foundAtoms.size() << " atoms" << endl;
        if (foundRows.empty()) {
            if (foundAtoms.empty()) {
                return nullptr;
            }
            else if (foundAtoms.size() == 1) {
                return &(storage = std::move(foundAtoms[0]));
            }
            else {
                return &(storage = ExpressionValue::superpose(std::move(foundAtoms)));
            }
        }

        for (auto & a: foundAtoms) {
            // We have both rows and atoms
            foundRows.emplace_back(Coord(), std::move(a));
        }

        //cerr << "got rows " << jsonEncode(foundRows) << endl;

        if (filter == GET_ALL || foundRows.size() == 1) {
            return &(storage = std::move(foundRows));
        }
        else {
            ExpressionValue unfiltered(std::move(foundRows));
            return &(storage = unfiltered.getFilteredDestructive(filter));
        }
    }
    
    std::vector<ExpressionValue> foundAtoms;
    StructValue foundRows;
};

template<typename Fn, typename StructValueT>
bool iterateStructured(StructValueT && vals,
                       const Coord & toFind,
                       Fn && onValue)
{
    for (auto && v: vals) {
        if (std::get<0>(v) == toFind) {
            if (!onValue(std::move(std::get<1>(v))))
                return false;
        }
    }

    return true;
}

} // file scope

const ExpressionValue *
ExpressionValue::
tryGetColumn(const Coord & columnName,
             ExpressionValue & storage,
             const VariableFilter & filter) const
{
    switch (type_) {
    case Type::STRUCTURED: {
        FilterAccumulator accum(filter);
        iterateStructured(*structured_, columnName, accum);
        return accum.extract(storage);
    }
    case Type::EMBEDDING: {
        return embedding_->tryGetNestedColumn(columnName, storage, ts_);
    }
    case Type::SUPERPOSITION: {
        return superposition_->tryGetNestedColumn(columnName, storage, ts_);
    }
    case Type::NONE:
    case Type::ATOM:
        return nullptr;
    }

    throw HttpReturnException(500, "Unknown expression value type");
}

ExpressionValue
ExpressionValue::
getColumn(const Coord & columnName, const VariableFilter & filter) const
{
    ExpressionValue storage;
    const ExpressionValue * val = tryGetColumn(columnName, storage, filter);
    if (!val)
        return ExpressionValue();
    else if (val == &storage)
        return std::move(storage);
    else return *val;
}

const ExpressionValue *
ExpressionValue::
tryGetNestedColumn(const ColumnName & columnName,
                   ExpressionValue & storage,
                   const VariableFilter & filter) const
{
    switch (type_) {
    case Type::STRUCTURED: {
        if (columnName.empty())
            return this;

        FilterAccumulator accum(filter);
        ColumnName tail = columnName.tail();

        auto onValue = [&] (const ExpressionValue & val) -> bool
            {
                if (columnName.size() == 1) {
                    return accum(val);
                }
                else {
                    ExpressionValue storage;
                    const ExpressionValue * res
                        = val.tryGetNestedColumn(tail, storage, filter);
                    if (res) {
                        if (res == &storage) {
                            return accum(std::move(storage));
                        }
                        else {
                            return accum(*res);
                        }
                    }
                    return true;  // skip it
                }
            };
        
        iterateStructured(*structured_, columnName[0], onValue);

        return accum.extract(storage);
    }
    case Type::EMBEDDING: {
        return embedding_->tryGetNestedColumn(columnName, storage, ts_);
    }
    case Type::SUPERPOSITION: {
        return superposition_->tryGetNestedColumn(columnName, storage, ts_);
    }
    case Type::NONE:
    case Type::ATOM:
        return nullptr;
    }
    throw HttpReturnException(500, "Unknown expression value type");
}

ExpressionValue
ExpressionValue::
getNestedColumn(const ColumnName & columnName, const VariableFilter & filter) const
{
    ExpressionValue storage;
    const ExpressionValue * val = tryGetNestedColumn(columnName, storage, filter);
    if (!val)
        return ExpressionValue();
    else if (val == &storage)
        return std::move(storage);
    else return *val;
}

std::vector<size_t>
ExpressionValue::
getEmbeddingShape() const
{
    switch (type_) {
    case Type::NONE:
    case Type::ATOM:
        return {};
    case Type::STRUCTURED:
        return { structured_->size() };
    case Type::EMBEDDING:
        return embedding_->dims_;
    case Type::SUPERPOSITION:
        return superposition_->latest().getEmbeddingShape();
    }

    throw HttpReturnException(500, "Unknown storage type for getEmbeddingShape()");
}

ExpressionValue
ExpressionValue::
reshape(std::vector<size_t> newShape) const
{
    switch (type_) {
    case Type::NONE:
    case Type::ATOM:
    case Type::STRUCTURED:
        throw HttpReturnException(500, "Cannot reshape non-embedding");
    case Type::SUPERPOSITION:
        return superposition_->latest().reshape(newShape);
    case Type::EMBEDDING: {
        size_t totalLength = 1;
        for (auto & s: newShape)
            totalLength *= s;

        if (rowLength() > totalLength) {
            throw HttpReturnException(400, "Attempt to enlarge embedding by resizing");
        }
        return ExpressionValue::embedding(ts_, embedding_->data_,
                                          embedding_->storageType_,
                                          std::move(newShape));
    }
    }

    throw HttpReturnException(500, "Unknown storage type for reshape()");
}

#if 1
ML::distribution<float, std::vector<float> >
ExpressionValue::
getEmbedding(ssize_t knownLength) const
{
    return getEmbeddingDouble(knownLength).cast<float>();
}

ML::distribution<double, std::vector<double> >
ExpressionValue::
getEmbeddingDouble(ssize_t knownLength) const
{
    // TODO: this is inefficient.  We should be able to have the
    // info function return us one that does it much more
    // efficiently.

    std::vector<std::pair<ColumnName, double> > features;
    if (knownLength != -1)
        features.reserve(knownLength);

    auto onAtom = [&] (const ColumnName & columnName,
                       const ColumnName & prefix,
                       const CellValue & val,
                       Date ts)
        {
            features.emplace_back(columnName, val.toDouble());
            return true;
        };

    forEachAtom(onAtom);

    ML::distribution<double> result;
    result.reserve(features.size());
    for (unsigned i = 0;  i < features.size();  ++i) {
        result.push_back(features[i].second);
        if (result.size() == knownLength)
            break;
    }

    if (knownLength != -1 && result.size() != knownLength)
        throw HttpReturnException(400, Utf8String("Expected ") + to_string(knownLength) +
            " elements in embedding, got " + to_string(result.size()));

    //cerr << "embedding result is " << result << endl;

    return result;
}

ML::distribution<double, std::vector<double> >
ExpressionValue::
getEmbedding(const ColumnName * knownNames, size_t len) const
{
    ML::distribution<double> features
        (len, std::numeric_limits<float>::quiet_NaN());

    // Index of value we're writing.  If they aren't in order, this will
    // be -1.
    int currentIndex = 0;

    /// If they're not in order, we create this index
    ML::Lightweight_Hash<uint64_t, int> columnIndex;
    
    /// Add a CellValue we extracted to the output.  This will also
    /// deal with non-ordered column names.
    auto addCell = [&] (const ColumnName & columnName, const CellValue & val)
        {
            double dbl = coerceTo(val, (double *)0);

            if (currentIndex >= 0) {
                // Up to now, they've been ordered
                if (currentIndex >= len)
                    throw HttpReturnException
                        (400, "too many columns extracting embedding: "
                         + columnName.toUtf8String(),
                         "extraColumn", columnName);
                
                if (knownNames[currentIndex] != columnName) {
                    // Set up the index of column names for non-ordered
                    // values.
                    for (size_t i = currentIndex;  i < len;  ++i) {
                        if (!columnIndex.insert({knownNames[i].newHash(), i})
                            .second) {
                            throw HttpReturnException
                                (400, "Column appears twice in embedding: '"
                                 + knownNames[i].toUtf8String()
                                 + "' appears at index "
                                 + std::to_string(columnIndex[knownNames[i].newHash()])
                                 + " and "
                                 + std::to_string(i));
                        }
                    }

                    currentIndex = -1;
                }
                else {
                    if (!val.empty()) {
                        features[currentIndex] = dbl;
                    }
                    currentIndex++;
                    return;
                }
            }
            
            // If we got here, we're getting columns out of the order that
            // they were passed in.  We look them up in the column index.
            auto it = columnIndex.find(columnName.newHash());
            if (it == columnIndex.end() || it->second == -1) {
                bool addedTwice
                    = (it != columnIndex.end() && it->second == -1)
                    || (std::find(knownNames, knownNames + len,
                                  columnName)
                        != knownNames + len);
                if (addedTwice) {
                    throw HttpReturnException
                        (400, "Column '" + columnName.toUtf8String()
                         + " was added twice to embedding",
                         "row", *this,
                         "knownNames",
                         vector<ColumnName>(knownNames, knownNames + len));
                }
                else {
                    throw HttpReturnException
                        (400, "Column '" + columnName.toUtf8String()
                         + "' was unknown for embedding",
                         "row", *this,
                         "knownNames",
                         vector<ColumnName>(knownNames, knownNames + len));
                }
            }

            features[it->second] = dbl;

            // Record as already done to detect double-adds of features
            // for the embedding.  The lightweight hash doesn't support
            // deleting elements, so we have to mark them as -1.
            it->second = -1;
        };

    switch (type_) {

    case Type::EMBEDDING: {
        std::vector<size_t> shape = getEmbeddingShape();
        size_t totalLength = 1;
        for (auto & s: shape)
            totalLength *= s;

        if (len != totalLength)
            throw HttpReturnException(400,
                                      "wrong number of columns for embedding",
                                      "shape", shape,
                                      "totalLength", totalLength,
                                      "numExpectedCols", len);

        bool allGood = shape.size() == 1;
        if (shape.size() == 1) {
            // For the single dimensional case we can optimize when the
            // column names are in order.
            for (size_t i = 0;  i < totalLength && allGood;  ++i) {
                if (knownNames[i] != Coords(Coord(i)))
                    allGood = false;
            }

            if (allGood) {
                getNumbers(embedding_->data_.get(),
                           embedding_->storageType_,
                           features.data(), totalLength);
            }
        }
        
        if (!allGood) {
            // Not the simple case (either out of order, or more than
            // one dimension).  Do it the slower way; at least we will
            // get the right result!
            auto onAtom = [&] (ColumnName & col, CellValue & val)
                {
                    addCell(col, val);
                    return true;
                };
            
            embedding_->forEachAtom(onAtom);
        }
        break;
    }

    case Type::STRUCTURED:
        for (auto & r: *structured_) {
            const ColumnName & columnName = std::get<0>(r);
            const ExpressionValue & val = std::get<1>(r);

            if (val.isAtom()) {
                addCell(columnName, val.getAtom());
            }
            else {
                auto onAtom = [&] (const Coords & columnName,
                                   const Coords & prefix,
                                   const CellValue & val,
                                   Date ts)
                    {
                        addCell(prefix + columnName, val);
                        return true;
                    };

                val.forEachAtom(onAtom, columnName);
            }
        }
        break;

    case Type::NONE:
    case Type::ATOM:
        throw HttpReturnException(400, "Cannot extract embedding from atom");
    case Type::SUPERPOSITION:
        return superposition_->latest().getEmbedding(knownNames, len);
    }

    return features;
}

std::vector<CellValue>
ExpressionValue::
getEmbeddingCell(ssize_t knownLength) const
{
    std::vector<CellValue> result;
    if (knownLength > 0)
        result.reserve(knownLength);

    if (type_ == Type::EMBEDDING) {
        ExcAssert(embedding_);
        size_t len = embedding_->length();
        for (size_t i = 0;  i < len;  ++i)
            result.emplace_back(embedding_->getValue(i));
        return result;
    }
    else if (type_ == Type::SUPERPOSITION) {
        return superposition_->latest().getEmbeddingCell(knownLength);
    }

    throw HttpReturnException(500, "getEmbeddingCell called for non-embedding");
}
#endif

void
ExpressionValue::
appendToRow(const ColumnName & columnName, MatrixNamedRow & row) const
{
    appendToRow(columnName, row.columns);
}

void
ExpressionValue::
appendToRow(const ColumnName & columnName, RowValue & row) const
{
    auto onAtom = [&] (const ColumnName & columnName,
                       const Coords & prefix,
                       const CellValue & val,
                       Date ts)
        {
            if (prefix == Coords()) {
                row.emplace_back(columnName, val, ts);
            }
            else if (columnName == ColumnName()) {
                row.emplace_back(prefix, val, ts);
            }
            else {
                row.emplace_back(prefix + columnName, val, ts);
            }
            return true;
        };

    forEachAtom(onAtom, columnName);
}

void
ExpressionValue::
appendToRow(const Coords & columnName, StructValue & row) const
{
    if (columnName.empty()) {
        auto onSubexpr = [&] (const Coord & columnName,
                              const ExpressionValue & val)
            {
                row.emplace_back(columnName, val);
                return true;
            };

        forEachColumn(onSubexpr);
    }
    else {
        // This function deals with if we want our value to be
        // x.y.z.  In that case, we have to structure it like
        // this: {"x": {"y": {"z": value } } }, in other words
        // create nested objects.  This recursive function creates
        // those nested objects.
        std::function<ExpressionValue (ExpressionValue inner,
                                       size_t scope)>
            wrapOutput = [&] (ExpressionValue inner,
                              ssize_t scope) -> ExpressionValue
            {
                if (scope == columnName.size()) {
                    return std::move(inner);
                }
                else {
                    StructValue row;
                    row.reserve(1);
                    row.emplace_back(columnName.at(scope),
                                     wrapOutput(std::move(inner), scope + 1));
                    return std::move(row);
                }
            };

        row.emplace_back(columnName[0], wrapOutput(*this, 1));
    }
}

void
ExpressionValue::
appendToRowDestructive(const Coords & columnName, StructValue & row)
{
    if (columnName.empty()) {
        auto onSubexpr = [&] (Coord & columnName,
                              ExpressionValue & val)
            {
                row.emplace_back(std::move(columnName), std::move(val));
                return true;
            };

        forEachColumnDestructive(onSubexpr);
    }
    else {
        // This function deals with if we want our value to be
        // x.y.z.  In that case, we have to structure it like
        // this: {"x": {"y": {"z": value } } }, in other words
        // create nested objects.  This recursive function creates
        // those nested objects.
        std::function<ExpressionValue (ExpressionValue inner,
                                       size_t scope)>
            wrapOutput = [&] (ExpressionValue inner,
                              ssize_t scope) -> ExpressionValue
            {
                if (scope == columnName.size()) {
                    return std::move(inner);
                }
                else {
                    StructValue row;
                    row.reserve(1);
                    row.emplace_back(columnName.at(scope),
                                     wrapOutput(std::move(inner), scope + 1));
                    return std::move(row);
                }
            };

        row.emplace_back(columnName[0], wrapOutput(std::move(*this), 1));
    }
}

void
ExpressionValue::
appendToRowDestructive(ColumnName & columnName, RowValue & row)
{
    switch (type_) {
    case Type::NONE:
        row.emplace_back(std::move(columnName), CellValue(), ts_);
        return;
    case Type::ATOM:
        row.emplace_back(std::move(columnName), stealAtom(), ts_);
        return;
    case Type::STRUCTURED:
    case Type::EMBEDDING:
    case Type::SUPERPOSITION:
        if (row.capacity() == 0)
            row.reserve(rowLength());
        else if (row.capacity() < row.size() + rowLength())
            row.reserve(row.capacity() * 2);

        if (columnName.empty()) {
            auto onSubexpr = [&] (ColumnName & innerColumnName,
                                  CellValue & val,
                                  Date ts)
                {
                    row.emplace_back(std::move(innerColumnName),
                                     std::move(val),
                                     ts);
                    return true;
                };
            
            forEachAtomDestructiveT(onSubexpr);
            return;
        }
        else {
            auto onSubexpr = [&] (ColumnName & innerColumnName,
                                  CellValue & val,
                                  Date ts)
                {
                    innerColumnName = columnName + std::move(innerColumnName);
                    row.emplace_back(std::move(innerColumnName),
                                     std::move(val),
                                     ts);
                    return true;
                };
            
            forEachAtomDestructiveT(onSubexpr);
            return;
        }
    }

    throw HttpReturnException(500, "Unknown storage type for appendToRowDestructive()");
}

size_t
ExpressionValue::
rowLength() const
{
    if (type_ == Type::STRUCTURED) {
        return structured_->size();
    }
    else if (type_ == Type::EMBEDDING) {
        return embedding_->length();
    }
    else if (type_ == Type::SUPERPOSITION) {
        return superposition_->length();
    }
    else throw HttpReturnException(500, "Attempt to access non-row as row",
                                   "value", *this);
}

void
ExpressionValue::
mergeToRowDestructive(StructValue & row)
{
    row.reserve(row.size() + rowLength());

    auto onSubexpr = [&] (Coord & columnName,
                          ExpressionValue & val)
        {
            row.emplace_back(std::move(columnName), std::move(val));
            return true;
        };

    forEachColumnDestructiveT(onSubexpr);
}

void 
ExpressionValue::
mergeToRowDestructive(RowValue & row)
{
    row.reserve(row.size() + rowLength());

    auto onSubexpr = [&] (ColumnName & columnName,
                          CellValue & val,
                          Date ts)
        {
            row.emplace_back(std::move(columnName), std::move(val), ts);
            return true;
        };

    forEachAtomDestructiveT(onSubexpr);
}

bool
ExpressionValue::
forEachAtom(const std::function<bool (const Coords & columnName,
                                      const Coords & prefix,
                                      const CellValue & val,
                                      Date ts) > & onAtom,
            const Coords & prefix) const
{
    switch (type_) {
    case Type::STRUCTURED: {
        for (auto & col: *structured_) {

            const ExpressionValue & val = std::get<1>(col);

            if (val.type_ == Type::NONE) {
                if (!onAtom(std::get<0>(col), prefix, CellValue(), val.getEffectiveTimestamp()))
                    return false;
            }
            else if (val.type_ == Type::ATOM) {
                if (!onAtom(std::get<0>(col), prefix, val.getAtom(), val.getEffectiveTimestamp()))
                    return false;
            }
            else {
                if (!val.forEachAtom(onAtom, prefix + std::get<0>(col)))
                    return false;
            }
        }
        return true;
    }
    case Type::EMBEDDING: {
        auto onCol = [&] (ColumnName & columnName, CellValue & val)
            {
                return onAtom(columnName, prefix, val, ts_);
            };
        
        return embedding_->forEachAtom(onCol);
    }
    case Type::SUPERPOSITION: {
        auto onCol = [&] (ColumnName & columnName, CellValue & val)
            {
                return onAtom(columnName, prefix, val, ts_);
            };
        
        return superposition_->forEachAtom(onCol);
    }
    case Type::NONE: {
        return onAtom(Coords(), prefix, CellValue(), ts_);
    }
    case Type::ATOM: {
        return onAtom(Coords(), prefix, cell_, ts_);
    }
    }

    throw HttpReturnException(500, "Unknown expression type",
                              "expression", *this,
                              "type", (int)type_);
}

bool
ExpressionValue::
forEachColumn(const std::function<bool (const Coord & columnName,
                                        const ExpressionValue & val)>
              & onColumn) const
{
    switch (type_) {
    case Type::STRUCTURED: {
        for (auto & col: *structured_) {
            const ExpressionValue & val = std::get<1>(col);
            if (!onColumn(std::get<0>(col), val))
                return false;
        }
        return true;
    }
    case Type::EMBEDDING: {
        return embedding_->forEachColumn(onColumn, ts_);
    }
    case Type::SUPERPOSITION: {
        return embedding_->forEachColumn(onColumn, ts_);
    }
    case Type::NONE:
    case Type::ATOM:
        // A non-row doesn't have columns, so this call doesn't make sense
        throw HttpReturnException(500, "Expected row expression",
                                  "expression", *this,
                                  "type", (int)type_);
    }

    throw HttpReturnException(500, "Unknown expression type",
                              "expression", *this,
                              "type", (int)type_);
}

bool
ExpressionValue::
forEachColumnDestructive(const std::function<bool (Coord & columnName, ExpressionValue & val)>
                                & onColumn) const
{
    return forEachColumnDestructiveT(onColumn);
}

template<typename Fn>
bool
ExpressionValue::
forEachColumnDestructiveT(Fn && onColumn) const
{
    switch (type_) {
    case Type::STRUCTURED: {
        if (structured_.unique()) {
            // We have the only reference in the shared pointer, AND we have
            // a non-const version of that expression.  This means that it
            // should be thread-safe to break constness and steal the result,
            // as otherwise we would have two references from different threads
            // with at least one non-const, which breaks thread safety.

            for (auto & col: const_cast<Structured &>(*structured_)) {
                ExpressionValue val(std::move(std::get<1>(col)));
                Coord name(std::move(std::get<0>(col)));
                if (!onColumn(name, val))
                    return false;
            }
        }
        else {
            for (auto & col: *structured_) {
                ExpressionValue val = std::get<1>(col);
                Coord name = std::get<0>(col);
                if (!onColumn(name, val))
                    return false;
            }
        }
        return true;
    }
    case Type::EMBEDDING: {
        auto onCol = [&] (Coord & columnName, ExpressionValue & val)
            {
                return onColumn(columnName, val);
            };
        
        return embedding_->forEachColumn(onCol, ts_);
    }
    case Type::SUPERPOSITION: {
        auto onCol = [&] (Coord & columnName, ExpressionValue & val)
            {
                return onColumn(columnName, val);
            };
        
        return superposition_->forEachColumn(onCol, ts_);
    }
    case Type::NONE:
    case Type::ATOM:
        throw HttpReturnException(500, "Expected row expression",
                                  "expression", *this,
                                  "type", (int)type_);
    }
    throw HttpReturnException(500, "Unknown expression type",
                              "expression", *this,
                              "type", (int)type_);
}

bool
ExpressionValue::
forEachAtomDestructive(const std::function<bool (Coords & columnName,
                                                 CellValue & val,
                                                 Date ts) > & onAtom)
{
    return forEachAtomDestructiveT(onAtom);
}

template<typename Fn>
bool
ExpressionValue::
forEachAtomDestructiveT(Fn && onAtom)
{
    switch (type_) {
    case Type::STRUCTURED: {
        if (structured_.unique()) {
            // We have the only reference in the shared pointer, AND we have
            // a non-const version of that expression.  This means that it
            // should be thread-safe to break constness and steal the result,
            // as otherwise we would have two references from different threads
            // with at least one non-const, which breaks thread safety.

            for (auto & col: const_cast<Structured &>(*structured_)) {
                auto onAtom2 = [&] (ColumnName & columnName,
                                    CellValue & val,
                                    Date ts)
                    {
                        Coords fullColumnName
                            = std::move(std::get<0>(col)) + std::move(columnName);
                        return onAtom(fullColumnName, val, ts);
                    };
                
                std::get<1>(col).forEachAtomDestructive(onAtom2);
            }
        }
        else {
            for (auto & col: *structured_) {
                auto onAtom2 = [&] (ColumnName columnName,
                                    ColumnName prefix,
                                    CellValue val,
                                    Date ts)
                    {
                        Coords fullColumnName
                            = std::move(prefix) + std::move(columnName);

                        return onAtom(fullColumnName, val, ts);
                    };
                
                std::get<1>(col).forEachAtom(onAtom2, std::get<0>(col));
            }
        }
        return true;
    }
    case Type::EMBEDDING: {
        auto onCol = [&] (Coords & columnName, CellValue & val)
            {
                return onAtom(columnName, val, ts_);
            };
        
        return embedding_->forEachAtom(onCol);
    }
    case Type::SUPERPOSITION: {
        auto onCol = [&] (Coords & columnName, CellValue & val)
            {
                return onAtom(columnName, val, ts_);
            };
        
        return superposition_->forEachAtom(onCol);
    }
    case Type::NONE: {
        Coords name;
        CellValue val;
        return onAtom(name, val, ts_);
    }
    case Type::ATOM: {
        Coords name;
        CellValue val = stealAtom();
        return onAtom(name, val, ts_);
    }
    }
    throw HttpReturnException(500, "Unknown expression type",
                              "expression", *this,
                              "type", (int)type_);
}

bool
ExpressionValue::
forEachSuperposedValue(const std::function<bool (const ExpressionValue & val)> & onValue) const
{
    switch (type_) {
    case Type::STRUCTURED: {

        // First, iterate through all the superposed values (with an empty
        // column name).
        size_t numAtoms = 0;
        StructValue nonAtoms;
        auto onColumn = [&] (const Coord & columnName,
                             const ExpressionValue & val)
            {
                if (!columnName.empty()) {
                    nonAtoms.emplace_back(columnName, val);
                    return true;
                }
                ++numAtoms;
                return onValue(val);
            };

        if (!forEachColumn(onColumn))
            return false;

        if (!nonAtoms.empty()) {
            // Secondly, return the non-atoms which are in their own row
            // valued object.
            return onValue(std::move(nonAtoms));
        }
        
        return true;
    }
    case Type::NONE:
        return true;
    case Type::EMBEDDING:
    case Type::ATOM:
        return onValue(*this);
    case Type::SUPERPOSITION:
        for (auto & v: superposition_->values) {
            if (!onValue(v))
                return false;
            return true;
        }
    }
        
    throw HttpReturnException(500, "Unknown expression type",
                              "expression", *this,
                              "type", (int)type_);
}

// Remove any duplicated columns according to the filter
const ExpressionValue &
ExpressionValue::
getFiltered(const VariableFilter & filter,
            ExpressionValue & storage) const
{
    if (filter == GET_ALL || empty() || isAtom() || type_ == Type::EMBEDDING)
        return storage = *this;

    if (type_ == Type::SUPERPOSITION) {
        switch (filter) {
        case GET_ANY_ONE:
            return superposition_->values[0];
        case GET_EARLIEST:
            return superposition_->earliest();
        case GET_LATEST:
            return superposition_->latest();
        case GET_ALL:
            assert(false);  // can't happen
        }
    }

    // This accumulates everything that's an atom, ie a straight atom or
    // elements of a superposition.
    FilterAccumulator atoms(filter);

    // This accumulates the row elements
    StructValue rows;

    auto onColumn = [&] (const Coord & col,
                         const ExpressionValue & val)
        {
            if (col.empty()) {
                atoms(val);
                return true;
            }

            FilterAccumulator accum(filter);
            accum(val);
            ExpressionValue storage;
            const ExpressionValue * output = accum.extract(storage);
            ExcAssert(output);
            if (output != &storage)
                rows.emplace_back(std::move(col), *output);
            else rows.emplace_back(std::move(col), std::move(storage));
            return true;
        };
    
    forEachColumn(onColumn);

    if (rows.empty()) {
        const ExpressionValue * output = atoms.extract(storage);
        if (output)
            return *output;
        else return (storage = ExpressionValue());
    }
    else {
        // Atoms get added to rows
        if (!atoms.empty()) {
            ExpressionValue storage2;
            const ExpressionValue * atomOutput = atoms.extract(storage2);
            if (atomOutput) {
                // TODO: move from storage
                rows.emplace_back(Coord(), *atomOutput);
            }
        }
    }
    
    return (storage = std::move(rows));
}

// Remove any duplicated columns according to the filter
ExpressionValue
ExpressionValue::
getFilteredDestructive(const VariableFilter & filter)
{
    if (filter == GET_ALL || empty() || isAtom() || type_ == Type::EMBEDDING)
        return std::move(*this);

    if (type_ == Type::SUPERPOSITION) {
        switch (filter) {
        case GET_ANY_ONE:
            return superposition_->values[0];
        case GET_EARLIEST:
            return superposition_->earliest();
        case GET_LATEST:
            return superposition_->latest();
        case GET_ALL:
            assert(false);  // can't happen
        }
    }

    // This accumulates everything that's an atom, ie a straight atom or
    // elements of a superposition.
    FilterAccumulator atoms(filter);

    // This accumulates the row elements
    StructValue rows;

    auto onColumn = [&] (Coord & col,
                         ExpressionValue & val)
        {
            if (col.empty()) {
                atoms(val);
                return true;
            }

            FilterAccumulator accum(filter);
            accum(val);
            ExpressionValue storage;
            const ExpressionValue * output = accum.extract(storage);
            ExcAssert(output);
            if (output != &storage)
                rows.emplace_back(std::move(col), *output);
            else rows.emplace_back(std::move(col), std::move(storage));
            return true;
        };
    
    forEachColumnDestructive(onColumn);

    if (rows.empty()) {
        ExpressionValue storage;
        const ExpressionValue * output = atoms.extract(storage);
        if (output) {
            if (output == &storage)
                return std::move(storage);
            else return *output;
        }
    }
    else {
        // Atoms get added to rows
        if (!atoms.empty()) {
            ExpressionValue storage2;
            const ExpressionValue * atomOutput = atoms.extract(storage2);
            if (atomOutput) {
                // TODO: move from storage
                rows.emplace_back(Coord(), *atomOutput);
            }
        }
    }
    
    return std::move(rows);
}

bool
ExpressionValue::
joinColumns(const ExpressionValue & val1,
            const ExpressionValue & val2,
            const OnMatchingColumn & onMatchingColumn,
            Outer outer)
{
    // TODO: we don't *at all* handle multiple versions of the current
    // column here.
    bool outerLeft = (outer == OUTER || outer == OUTER_LEFT);
    bool outerRight = (outer == OUTER || outer == OUTER_RIGHT);

    RowValue row1, row2;
    val1.appendToRow(ColumnName(), row1);
    val2.appendToRow(ColumnName(), row2);

    if (row1.size() == row2.size()) {
        bool matchingNames = true;

        // Assume they have the same keys and exactly one of each; if not we
        // have to restart
        ML::Lightweight_Hash_Set<uint64_t> colsFound;

        for (size_t i = 0;  i < row1.size(); ++i) {
            const ColumnName & col1 = std::get<0>(row1[i]);
            const ColumnName & col2 = std::get<0>(row2[i]);
            if (col1 != col2) {
                // non-matching column name
                matchingNames = false;
                break;
            }
            if (colsFound.insert(col1.hash()).second == false) {
                // duplicate column name
                matchingNames = false;
                break;
            }
        }

        if (matchingNames) {
            // Easy case: one single value of each one
            for (size_t i = 0;  i < row1.size(); ++i) {
                const ColumnName & col = std::get<0>(row1[i]);

                std::pair<CellValue, Date>
                    val1(std::get<1>(row1[i]), std::get<2>(row1[i]));
                std::pair<CellValue, Date>
                    val2(std::get<1>(row2[i]), std::get<2>(row2[i]));

                if (!onMatchingColumn(col, &val1, &val2, 1, 1))
                    return false;
            }
            return true;
        }
    }

    // Names don't match or there are duplicates.  We need to sort them and
    // iterate across batches of matching entries.

    std::sort(row1.begin(), row1.end());
    std::sort(row2.begin(), row2.end());

    auto it1 = row1.begin(), end1 = row1.end();
    auto it2 = row2.begin(), end2 = row2.end();

    while (it1 != end1 && it2 != end2) {
        const ColumnName & col1 = std::get<0>(*it1);
        const ColumnName & col2 = std::get<0>(*it2);
        
        const ColumnName & minCol = col1 < col2 ? col1 : col2;

        // If we don't have a match on each side, check for the
        // outer condition and if it doesn't match, continue on later.
        if (minCol != col1 && !outerLeft) {
            while (it2 != end2 && std::get<0>(*it2) < col1)
                ++it2;
            continue;
        }
        if (minCol != col2 && !outerRight) {
            while (it1 != end1 && std::get<0>(*it1) < col2)
                ++it2;
            continue;
        }

        std::vector<std::pair<CellValue, Date> > lvals, rvals;

        while (it1 != end1 && std::get<0>(*it1) == minCol) {
            lvals.emplace_back(std::get<1>(*it1), std::get<2>(*it1));
            ++it1;
        }
        while (it2 != end2 && std::get<0>(*it2) == minCol) {
            rvals.emplace_back(std::get<1>(*it2), std::get<2>(*it2));
            ++it2;
        }
        
        // Present the examples
        if (!onMatchingColumn(minCol, &lvals[0], &rvals[0],
                              lvals.size(), rvals.size()))
            return false;
    }
    
    while (outerLeft && it1 != end1) {
        const ColumnName & col1 = std::get<0>(*it1);

        std::vector<std::pair<CellValue, Date> > lvals, rvals;

        while (it1 != end1 && std::get<0>(*it1) == col1) {
            lvals.emplace_back(std::get<1>(*it1), std::get<2>(*it1));
            ++it1;
        }
        
        // Present the examples
        if (!onMatchingColumn(col1, &lvals[0], &rvals[0],
                              lvals.size(), rvals.size()))
            return false;
    }

    while (outerRight && it2 != end2) {
        const ColumnName & col2 = std::get<0>(*it2);

        std::vector<std::pair<CellValue, Date> > lvals, rvals;

        while (it2 != end2 && std::get<0>(*it2) == col2) {
            rvals.emplace_back(std::get<1>(*it2), std::get<2>(*it2));
            ++it2;
        }

        // Present the examples
        if (!onMatchingColumn(col2, &lvals[0], &rvals[0],
                              lvals.size(), rvals.size()))
            return false;
    }

    return true;
}

std::pair<bool, Date>
ExpressionValue::
hasKey(const Utf8String & key) const
{
    switch (type_) {
    case Type::NONE:
    case Type::ATOM:
        return { false, Date::negativeInfinity() };
    case Type::STRUCTURED: 
    case Type::SUPERPOSITION: 
    case Type::EMBEDDING: {
        // TODO: for Embedding, we can do much, much better
        Date outputDate = Date::negativeInfinity();
        auto onExpr = [&] (const Coords & columnName,
                           const ExpressionValue & val)
            {
                if (columnName == Coords(Coord(key))) {
                    outputDate = val.getEffectiveTimestamp();
                    return false;
                }

                return true;
            };
     
        // Can't be with next line due to sequence points
        bool result = !forEachColumn(onExpr);
        return { result, outputDate };
    }
#if 0
    case Type::EMBEDDING: {
        // Look for a key of the form [x,y,z]
        int indexes[10];
        int n = -1;
        int64_t current = 0;

        for (char32_t c: key) {
            if (n == -1) {
                if (c != '[')
                    return { false, Date::negativeInfinity() };
                n = 0;
            }
            if (c == 0) {
            }
        }
    }
#endif
    }

    throw HttpReturnException(500, "Unknown expression type",
                              "expression", *this,
                              "type", (int)type_);
}

std::pair<bool, Date>
ExpressionValue::
hasValue(const ExpressionValue & val) const
{
    switch (type_) {
    case Type::NONE:
    case Type::ATOM:
        return { false, Date::negativeInfinity() };
    case Type::STRUCTURED: 
    case Type::SUPERPOSITION: 
    case Type::EMBEDDING: {
        // TODO: for embedding, we can do much, much better
        Date outputDate = Date::negativeInfinity();
        auto onExpr = [&] (const Coords & columnName,
                           const ExpressionValue & value)
            {
                if (val == value) {
                    outputDate = value.getEffectiveTimestamp();
                    return false;
                }

                return true;
            };
        
        // Can't be with next line due to sequence points
        bool result = !forEachColumn(onExpr);
        return { result, outputDate };
    }
    }

    throw HttpReturnException(500, "Unknown expression type",
                              "expression", *this,
                              "type", (int)type_);
}

size_t
ExpressionValue::
hash() const
{
    switch (type_) {
    case Type::NONE:
        return CellValue().hash();
    case Type::ATOM:
        return cell_.hash();
    case Type::STRUCTURED:
    case Type::EMBEDDING:
    case Type::SUPERPOSITION:
        // TODO: a more reasonable speed in hashing
        return jsonHash(jsonEncode(*this));
    }
    throw HttpReturnException(500, "Unknown expression type",
                              "expression", *this,
                              "type", (int)type_);
}

void
ExpressionValue::
initInt(int64_t intValue, Date ts)
{
    type_ = Type::NONE;
    setAtom(intValue, ts);
}

void
ExpressionValue::
initUInt(uint64_t intValue, Date ts)
{
    type_ = Type::NONE;
    setAtom(intValue, ts);
}

void
ExpressionValue::
initStructured(Structured value) noexcept
{
    // Do we need sorting, etc?
    bool needsSorting = true;  // TODO: detect this; it will make things much faster

    if (needsSorting) {
        // Deduplicate...

        // Sort by row name then value
        std::sort(value.begin(), value.end());

        Structured newValue;
        newValue.reserve(value.size());

        // Remove dups
        for (ssize_t i = 0;  i < value.size();  /* no inc */) {
            // Find the end of the range of dups
            ssize_t j = i + 1;
            for (; j < value.size() && std::get<0>(value[i]) == std::get<0>(value[j]);
                 ++j);

            if (i == 0 && j == value.size()) {
                const Coord & key = std::get<0>(value[i]);

                if (key.empty()) {
                    // All have the same key.  We have one single element as a
                    // superposition.  If we continue, we'll get into an infinite
                    // loop.
                    value.swap(newValue);  // it will be swapped back...
                    break;
                }
            }


            if (j == i + 1) {
                newValue.emplace_back(std::move(value[i]));
                i = j;
            }
            else {
                std::vector<ExpressionValue> vals;
                vals.reserve(j - i);
                for (; i < j;  ++i) {
                    vals.emplace_back(std::move(std::get<1>(value[i])));
                }
                newValue.emplace_back(std::get<0>(value[i - 1]),
                                      superpose(std::move(vals)));
            }
        }

        value.swap(newValue);
    }

    initStructured(std::make_shared<Structured>(std::move(value)));
}

void
ExpressionValue::
initStructured(std::shared_ptr<const Structured> value) noexcept
{
    assertType(Type::NONE);
    ts_ = Date();
    if (value->size() == 0) {
        ts_ = Date::notADate();
    }
    else {
        for (auto& r : *value) {
            ts_ = std::max(std::get<1>(r).getEffectiveTimestamp(), ts_);
        }
    }

    // In the case of a superposition, this isn't true
    //for (auto & v: *value) {
    //    ExcAssert(!std::get<0>(v).empty());
    //}

    new (storage_) std::shared_ptr<const Structured>(std::move(value));
    type_ = Type::STRUCTURED;
}

static const CellValue EMPTY_CELL;

const CellValue &
ExpressionValue::
getAtom() const
{
    if (type_ == Type::NONE)
        return EMPTY_CELL;
    assertType(Type::ATOM);
    return cell_;
}

CellValue
ExpressionValue::
stealAtom()
{
    if (type_ == Type::NONE)
        return CellValue();
    assertType(Type::ATOM);
    return std::move(cell_);
}

CellValue
ExpressionValue::
coerceToAtom() const
{
    switch (type_) {
    case Type::NONE:
        return EMPTY_CELL;
    case Type::ATOM:
        return cell_;
    case Type::STRUCTURED:
        ExcAssertEqual(structured_->size(), 1);
        return std::get<1>((*structured_)[0]).getAtom();
    case Type::EMBEDDING:
        ExcAssertEqual(embedding_->length(), 1);
        return embedding_->getValue(0);
    case Type::SUPERPOSITION:
        return superposition_->values[0].getAtom();
    }

    throw HttpReturnException(500, "coerceToAtom: unknown expression type");

    return EMPTY_CELL;
}

const ExpressionValue::Structured &
ExpressionValue::
getStructured() const
{
    assertType(Type::STRUCTURED);
    return *structured_;
}

#if 0
ExpressionValue::Structured
ExpressionValue::
stealStructured()
{
    assertType(Type::STRUCTURED);
    type_ = Type::NONE;
    return std::move(*structured_);
}
#endif

CellValue
ExpressionValue::
coerceToString() const
{
    if (type_ == Type::NONE)
        return CellValue();
    return toUtf8String();
}

CellValue
ExpressionValue::
coerceToInteger() const
{
    if (type_ != Type::ATOM)
        return CellValue();
    return cell_.coerceToInteger();
}

CellValue
ExpressionValue::
coerceToNumber() const
{
    if (type_ != Type::ATOM)
        return CellValue();
    return cell_.coerceToNumber();
}

CellValue
ExpressionValue::
coerceToBoolean() const
{
    if (type_ != Type::ATOM)
        return CellValue();
    return cell_.coerceToBoolean();
}

CellValue
ExpressionValue::
coerceToTimestamp() const
{
    if (type_ != Type::ATOM)
        return CellValue();
    return cell_.coerceToTimestamp();
}

CellValue
ExpressionValue::
coerceToBlob() const
{
    if (type_ != Type::ATOM)
        return CellValue();
    return cell_.coerceToBlob();
}

Coords
ExpressionValue::
coerceToPath() const
{
    if (empty())
        return Coords();
    else if (isAtom()) {
        return Coord(getAtom().coerceToPathElement());
    }
    else {
        vector<CellValue> vals = getEmbeddingCell();
        vector<Coord> coords;
        coords.reserve(vals.size());
        for (auto & v: vals) {
            coords.emplace_back(v.coerceToPathElement());
        }
        return Coords(std::make_move_iterator(coords.begin()),
                      std::make_move_iterator(coords.end()));
    }
}

void
ExpressionValue::
setAtom(CellValue value, Date ts)
{
    initAtom(std::move(value), ts);
}

vector<std::pair<ColumnName, CellValue> >
asRow(const ExpressionValue & expr)
{
    vector<std::pair<ColumnName, CellValue> > row;
    auto onAtom = [&] (const ColumnName & columnName,
                       const ColumnName & prefix,
                       const CellValue & val,
                       Date ts)
        {
            row.emplace_back(columnName, val);
            return true;
        };
    expr.forEachAtom(onAtom);
    return row;
}

template <typename T, template <typename> class Compare> 
typename Compare<T>::result_type 
compare_t(const ExpressionValue & left, const ExpressionValue & right)
{
    T leftRow = asRow(left);
    T rightRow = asRow(right);
    return ML::compare_sorted(leftRow, rightRow, Compare<T>());
}

int
ExpressionValue::
compare(const ExpressionValue & other) const
{
    if (type_ < other.type_)
        return -1;
    else if (type_ > other.type_)
        return 1;
    
    switch (type_) {
    case Type::NONE: return 0;
    case Type::ATOM:
        //cerr << "getAtom() 1 = " << getAtom() << endl;
        //cerr << "getAtom() 2 = " << other.getAtom() << endl;
        //cerr << "atom compare returned " << getAtom().compare(other.getAtom()) << endl;
        //cerr << "reverse atom compare returned " << other.getAtom().compare(getAtom()) << endl;
        return cell_.compare(other.cell_);
    case Type::STRUCTURED: {
        auto leftRow = getStructured();
        auto rightRow = other.getStructured();
        return ML::compare_sorted(leftRow, rightRow, ML::compare<Structured>());
    }
    case Type::SUPERPOSITION:
    case Type::EMBEDDING: {
        return compare_t<vector<pair<ColumnName, CellValue> >, ML::compare>(*this, other);
    }
    }
    
    throw HttpReturnException(400, "unknown ExpressionValue type");
}

bool
ExpressionValue::
operator == (const ExpressionValue & other) const
{
    if (type_ != other.type_)
        return false;
    switch (type_) {
    case Type::NONE: return true;
    case Type::ATOM: return cell_ == other.cell_;
    case Type::STRUCTURED: {
        auto leftRow = getStructured();
        auto rightRow = other.getStructured();
        return ML::compare_sorted(leftRow, rightRow, std::equal_to<Structured>());
    }
    case Type::SUPERPOSITION:
    case Type::EMBEDDING: {
        return compare_t<vector<pair<ColumnName, CellValue> >, equal_to>(*this, other);
    }
    }
    throw HttpReturnException(400, "unknown ExpressionValue type " + to_string((int)type_));
}

bool
ExpressionValue::
operator <  (const ExpressionValue & other) const
{
    if (type_ < other.type_)
        return true;
    if (type_ > other.type_)
        return false;

    switch (type_) {
    case Type::NONE: return false;
    case Type::ATOM: return cell_ < other.cell_;
    case Type::STRUCTURED:  {
        auto leftRow = getStructured();
        auto rightRow = other.getStructured();
        return ML::compare_sorted(leftRow, rightRow, std::less<Structured>());
    }
    case Type::SUPERPOSITION:
    case Type::EMBEDDING: {
        return compare_t<vector<pair<ColumnName, CellValue> >, less>(*this, other);
    }
    }
    throw HttpReturnException(400, "unknown ExpressionValue type");
}

std::shared_ptr<ExpressionValueInfo>
ExpressionValue::
getSpecializedValueInfo() const
{
    switch (type_) {
    case Type::NONE:
        return std::make_shared<EmptyValueInfo>();
    case Type::ATOM:
        // TODO: specialize for concrete type
        return std::make_shared<AtomValueInfo>();
    case Type::STRUCTURED:
        // TODO: specialize for concrete value.  Currently we just say
        // "it's a row with some values we don't know about yet"
        return std::make_shared<RowValueInfo>(vector<KnownColumn>(), SCHEMA_OPEN);
    case Type::SUPERPOSITION:
        return superposition_->getSpecializedValueInfo();
    case Type::EMBEDDING:
        return embedding_->getSpecializedValueInfo();
    }
    throw HttpReturnException(400, "unknown ExpressionValue type");
}

using Datacratic::getDefaultDescriptionShared;

namespace {
auto cellDesc = getDefaultDescriptionShared((CellValue *)0);
auto structuredDesc = getDefaultDescriptionShared((ExpressionValue::Structured *)0);
auto noTsDesc = getExpressionValueDescriptionNoTimestamp();
auto dateDesc = getDefaultDescriptionShared((Date *)0);
} // file scope

void
ExpressionValue::
extractJson(JsonPrintingContext & context) const
{
    switch (type_) {
    case ExpressionValue::Type::NONE:
        context.writeNull();
        return;

    case ExpressionValue::Type::ATOM:
        cellDesc->printJsonTyped(&cell_, context);
        return;

    case ExpressionValue::Type::STRUCTURED: {
        context.startObject();

        std::map<Coord, ML::compact_vector<int, 2> > vals;

        for (int i = 0;  i < structured_->size();  ++i) {
            // We need to deal with doubled values
            vals[std::get<0>((*structured_)[i])].push_back(i);
        }

        for (auto & v: vals) {
            
            if (v.first.hasStringView()) {
                const char * start;
                size_t len;

                std::tie(start, len) = v.first.getStringView();
                
                context.startMember(start, len);
            }
            else {
                context.startMember(v.first.toUtf8String());
            }
         
            if (v.second.size() == 1) {
                std::get<1>((*structured_)[v.second.front()])
                    .extractJson(context);
            }
            else {
                // Need to merge them together
                Json::Value jval;

                for (int index: v.second) {
                    const ExpressionValue & v = std::get<1>((*structured_)[index]);
                    Json::Value val = v.extractJson();
                    for (auto it = val.begin();  it != val.end();  ++it) {
                        jval[it.memberName()] = *it;
                    }
                }
                
                context.writeJson(jval);
            }
        }

        context.endObject();

        return;
    }
    case ExpressionValue::Type::SUPERPOSITION:
        superposition_->extractJson(context);
        return;
    case ExpressionValue::Type::EMBEDDING: {
        embedding_->extractJson(context);
        return;
    }
    }
    throw HttpReturnException(400, "unknown ExpressionValue type");
 }

Json::Value
ExpressionValue::
extractJson() const
{
    Json::Value result;
    StructuredJsonPrintingContext context(result);
    extractJson(context);
    return result;
}

/** Value description that includes the timestamp.  It serializes as an array with 
    [ value, timestamp ].
*/
struct ExpressionValueDescription
    : public ValueDescriptionT<ExpressionValue> {
    virtual void parseJsonTyped(ExpressionValue * val,
                                JsonParsingContext & context) const;
    virtual void printJsonTyped(const ExpressionValue * val,
                                JsonPrintingContext & context) const;
    virtual bool isDefaultTyped(const ExpressionValue * val) const;
};

/** Value description not including the timestamp.  It serializes as just the
    value.  When reading back, the timestamp will be set to NaD.
*/
struct ExpressionValueDescriptionNoTimestamp
    : public ValueDescriptionT<ExpressionValue> {
    virtual void parseJsonTyped(ExpressionValue * val,
                                JsonParsingContext & context) const;
    virtual void printJsonTyped(const ExpressionValue * val,
                                JsonPrintingContext & context) const;
    virtual bool isDefaultTyped(const ExpressionValue * val) const;
};

std::shared_ptr<ValueDescriptionT<ExpressionValue> >
getExpressionValueDescriptionNoTimestamp()
{
    return std::make_shared<ExpressionValueDescriptionNoTimestamp>();
}

void
ExpressionValueDescription::
parseJsonTyped(ExpressionValue * val,
               JsonParsingContext & context) const
{
    int el = 0;
    auto onElement = [&] ()
        {
            if (el == 0)
                noTsDesc->parseJsonTyped(val, context);
            else if (el == 1)
                dateDesc->parseJsonTyped(&val->ts_, context);
            else context.exception("expected 2 element array");

            ++el;
        };

    context.forEachElement(onElement);
    
    if (el != 2)
        context.exception("expected 2 element array");
}

void
ExpressionValueDescription::
printJsonTyped(const ExpressionValue * val,
               JsonPrintingContext & context) const
{
    context.startArray(2);
    context.newArrayElement();
    noTsDesc->printJsonTyped(val, context);
    context.newArrayElement();
    dateDesc->printJsonTyped(&val->ts_, context);
    context.endArray();
}

bool
ExpressionValueDescription::
isDefaultTyped(const ExpressionValue * val) const
{
    return false;
}

void
ExpressionValueDescriptionNoTimestamp::
parseJsonTyped(ExpressionValue * val,
               JsonParsingContext & context) const
{
    Date ts = Date::notADate();
        
    if (context.isNull()) {
        context.expectNull();
        *val = ExpressionValue(nullptr, ts);
    }
    else if (context.isString()) {
        *val = ExpressionValue(context.expectStringUtf8(), ts);
    }
    else if (context.isArray()) {
        Json::Value j = context.expectJson();
        *val = ExpressionValue(j, ts);
    }
    else if (context.isObject()) {
        Json::Value j = context.expectJson();
        *val = ExpressionValue(j, ts);
    }
    else if (context.isBool()) {
        *val = ExpressionValue(context.expectBool(), ts);
    }
    else if (context.isUnsigned()) {
        *val = ExpressionValue(context.expectUnsignedLongLong(), ts);
    }
    else if (context.isInt()) {
        *val = ExpressionValue(context.expectLongLong(), ts);
    }
    else {
        *val = ExpressionValue(context.expectDouble(), ts);
    }
}

void
ExpressionValueDescriptionNoTimestamp::
printJsonTyped(const ExpressionValue * val,
               JsonPrintingContext & context) const
{
    switch (val->type_) {
    case ExpressionValue::Type::NONE:
        context.writeNull();
        return;
    case ExpressionValue::Type::ATOM:
        cellDesc->printJsonTyped(&val->cell_, context);
        return;
    case ExpressionValue::Type::STRUCTURED:
        structuredDesc->printJsonTyped(val->structured_.get(), context);
        return;
    case ExpressionValue::Type::SUPERPOSITION:
        val->superposition_->writeJson(context);
        return;
    case ExpressionValue::Type::EMBEDDING: {
        val->embedding_->writeJson(context);
        return;
    }
    }
    throw HttpReturnException(400, "unknown ExpressionValue type");
}

bool
ExpressionValueDescriptionNoTimestamp::
isDefaultTyped(const ExpressionValue * val) const
{
    return val->empty();
}

std::string
ExpressionValue::
print(Type t)
{
    switch (t)  {
    case Type::NONE:      return "empty";
    case Type::ATOM:      return "atomic value";
    case Type::STRUCTURED:       return "structured";
    case Type::EMBEDDING: return "embedding";
    case Type::SUPERPOSITION: return "superposition";
    default:
        throw HttpReturnException(400, "Unknown ExpressionValue type: "
                                  + std::to_string((int)t));
    }
}

void
ExpressionValue::
assertType(Type requested, const std::string & details) const
{
    if(requested != type_) {
        std::string msg = "Cannot convert value of type "
            "'" + print(type_) + "' to "
            "'" + print(requested) + "'";
        if(!details.empty()) {
            msg += " (" + details+ ")";
        }

        throw HttpReturnException(400, msg);
    }
}


// Get a value description for values
DEFINE_VALUE_DESCRIPTION_NS(ExpressionValue, ExpressionValueDescription);

std::ostream & operator << (std::ostream & stream, const ExpressionValue & val)
{
    return stream << jsonEncode(val);
}


/*****************************************************************************/
/* TEMPLATE INSTANTIATIONS                                                   */
/*****************************************************************************/

template class ExpressionValueInfoT<float>;
template class ExpressionValueInfoT<double>;
template class ExpressionValueInfoT<CellValue>;
template class ExpressionValueInfoT<std::string>;
template class ExpressionValueInfoT<Utf8String>;
template class ExpressionValueInfoT<std::vector<unsigned char> >;
template class ExpressionValueInfoT<int64_t>;
template class ExpressionValueInfoT<uint64_t>;
template class ExpressionValueInfoT<char>;
template class ExpressionValueInfoT<Date>;
template class ScalarExpressionValueInfoT<float>;
template class ScalarExpressionValueInfoT<double>;
template class ScalarExpressionValueInfoT<CellValue>;
template class ScalarExpressionValueInfoT<std::string>;
template class ScalarExpressionValueInfoT<Utf8String>;
template class ScalarExpressionValueInfoT<std::vector<unsigned char> >;
template class ScalarExpressionValueInfoT<int64_t>;
template class ScalarExpressionValueInfoT<uint64_t>;
template class ScalarExpressionValueInfoT<char>;
template class ScalarExpressionValueInfoT<Date>;

template class ExpressionValueInfoT<RowValue>;
template class ExpressionValueInfoT<ExpressionValue>;
template class ExpressionValueInfoT<ML::distribution<double, std::vector<double> > >;


/*****************************************************************************/
/* SEARCH ROW                                                                */
/*****************************************************************************/

template<typename Key>
const ExpressionValue *
doSearchRow(const std::vector<std::tuple<Key, CellValue, Date> > & columns,
            const Key & key,
            const VariableFilter & filter,
            ExpressionValue & storage)
{
    int index = -1;

    //cerr << "doSearchRow columns " << jsonEncode(columns) << " key " << key << endl;

    switch (filter) {
    case GET_ANY_ONE:
        for (unsigned i = 0;  i < columns.size();  ++i) {
            const auto & c = columns[i];
            if (std::get<0>(c) == key) {
                index = i;
                break;
            }
        }
        break;
        
    case GET_EARLIEST: {
        Date foundDate;

        for (unsigned i = 0;  i < columns.size();  ++i) {
            const auto & c = columns[i];
            
            if (std::get<0>(c) == key) {
                if (index == -1
                    || std::get<2>(c) < foundDate
                    || (std::get<2>(c) == foundDate
                        && std::get<1>(c) < std::get<1>(columns[index]))) {
                    index = i;
                    foundDate = std::get<2>(c);
                }
            }
        }
        break;
    }

    case GET_LATEST: {
        Date foundDate;

        for (unsigned i = 0;  i < columns.size();  ++i) {
            const auto & c = columns[i];
            
            if (std::get<0>(c) == key) {
                if (index == -1
                    || std::get<2>(c) > foundDate
                    || (std::get<2>(c) == foundDate
                        && std::get<1>(c) < std::get<1>(columns[index]))) {
                    index = i;
                    foundDate = std::get<2>(c);
                }
            }
        }
        break;
    }

    case GET_ALL: {
        RowValue row;
        for (unsigned i = 0;  i < columns.size();  ++i) {
            const auto & c = columns[i];
            
            if (std::get<0>(c) == key) {
                index = i;
                row.push_back(c);
            }
        }
        
        // TODO - we may want to revisit this
        // when only one value is found, return a scalar not a row
        if (row.size() > 1)
            return &(storage = std::move(ExpressionValue(std::move(row))));
        break;
    }

    default:
        throw HttpReturnException
            (500, "Unknown variable filter not implemented for datasets");
    }

    if (index == -1)
        return nullptr;
    
    return &(storage = std::move(ExpressionValue(std::get<1>(columns[index]),
                                                 std::get<2>(columns[index]))));
}

const ExpressionValue *
searchRow(const std::vector<std::tuple<ColumnName, CellValue, Date> > & columns,
          const ColumnName & key,
          const VariableFilter & filter,
          ExpressionValue & storage)
{
    return doSearchRow(columns, key, filter, storage);
}

template<typename Key>
const ExpressionValue *
doSearchRow(const std::vector<std::tuple<Key, ExpressionValue> > & columns,
            const Key & key,
            const VariableFilter & filter,
            ExpressionValue & storage)
{
    int index = -1;

    switch (filter) {
    case GET_ANY_ONE:
        for (unsigned i = 0;  i < columns.size();  ++i) {
            const auto & c = columns[i];
            if (std::get<0>(c) == key) {
                index = i;
                break;
            }
        }
        break;
        
    case GET_EARLIEST: {
        Date foundDate;

        for (unsigned i = 0;  i < columns.size();  ++i) {
            const auto & c = columns[i];
            
            if (std::get<0>(c) == key) {
                const ExpressionValue & v = std::get<1>(c);
                Date ts = v.getEffectiveTimestamp();
                if (index == -1 || v.isEarlier(ts, std::get<1>(columns[index]))){
                    index = i;
                    foundDate = ts;
                }
            }
        }
        break;
    }

    case GET_LATEST: {
        Date foundDate;

        for (unsigned i = 0;  i < columns.size();  ++i) {
            const auto & c = columns[i];
            
            if (std::get<0>(c) == key) {
                const ExpressionValue & v = std::get<1>(c);
                Date ts = v.getEffectiveTimestamp();
                if (index == -1 || v.isLater(ts, std::get<1>(columns[index]))){
                    index = i;
                    foundDate = ts;
                }
            }
        }

        break;
    }

    case GET_ALL: {
        StructValue row;

        for (unsigned i = 0;  i < columns.size();  ++i) {
            const auto & c = columns[i];
            if (std::get<0>(c) == key) {
                row.emplace_back(Coord(), std::get<1>(c));
                index = i;
            }
        }

        if (row.size() == 0) {
            return nullptr;
        }
        else if (row.size() == 1) {
            break;
        }
        else return &(storage = std::move(row));
    }

    default:
        throw HttpReturnException(500, "Unknown GET_ALL not implemented for datasets");
    }

    if (index == -1)
        return nullptr;
    
    return &std::get<1>(columns[index]);
}

const ExpressionValue *
searchRow(const std::vector<std::tuple<Coord, ExpressionValue> > & columns,
          const Coord & key,
          const VariableFilter & filter,
          ExpressionValue & storage)
{
    FilterAccumulator accum(filter);
    iterateStructured(columns, key, accum);
    if (accum.empty())
        return nullptr;
    return accum.extract(storage);
}

const ExpressionValue *
searchRow(const std::vector<std::tuple<Coord, ExpressionValue> > & columns,
          const ColumnName & key,
          const VariableFilter & filter,
          ExpressionValue & storage)
{
    ExcAssert(!key.empty());

    FilterAccumulator accum(filter);
    ColumnName tail = key.tail();

    auto onValue = [&] (const ExpressionValue & val) -> bool
        {
            if (key.size() == 1) {
                return accum(val);
            }
            else {
                return accum(val.getNestedColumn(tail, filter));
            }
        };

    iterateStructured(columns, key.head(), onValue);

    if (accum.empty())
        return nullptr;
    
    return accum.extract(storage);
}

/*****************************************************************************/
/* NAMED ROW VALUE                                                           */
/*****************************************************************************/

DEFINE_STRUCTURE_DESCRIPTION(NamedRowValue);

NamedRowValueDescription::
NamedRowValueDescription()
{
    addField("rowName", &NamedRowValue::rowName, "Name of the row");
    addField("rowHash", &NamedRowValue::rowHash, "Hash of the row");
    addField("columns", &NamedRowValue::columns, "Columns active for this row");
}

MatrixNamedRow
NamedRowValue::flattenDestructive()
{
    MatrixNamedRow result;

    result.rowName = std::move(rowName);
    result.rowHash = std::move(rowHash);

    for (auto & c: columns) {
        Coord & fieldName = std::get<0>(c);
        ExpressionValue & val = std::get<1>(c);
        Coords columnName(std::move(fieldName));
        val.appendToRowDestructive(columnName, result.columns);
    }
    
    return result;
}

} // namespace MLDB
} // namespace Datacratic
