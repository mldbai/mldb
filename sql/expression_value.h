/** expression_value.h                                             -*- C++ -*-
    Jeremy Barnes, 14 February 2015

    This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

    Code for the type that holds the value of an expression.
*/

#pragma once

#include "dataset_fwd.h"
#include "mldb/types/value_description_fwd.h"
#include "mldb/types/date.h"
#include "mldb/arch/demangle.h"
#include "mldb/base/exc_assert.h"
#include "cell_value.h"
#include <cstdint>

// NOTE TO MLDB DEVELOPERS: This is an API header file.  No includes
// should be added, especially value_description.h.


namespace ML {
template<typename T, typename Underlying>
struct distribution;
} // namespace ML

namespace Datacratic {

struct Date;

inline bool operator == (const std::string & str1, const Id & str2)
{
    return str2.stringEqual(str1);
}

inline bool operator != (const std::string & str1, const Id & str2)
{
    return !str2.stringEqual(str1);
}

inline bool operator <  (const std::string & str1, const Id & str2)
{
    return str2.stringGreaterEqual(str1);
}

inline bool operator == (const Utf8String & str1, const Id & str2)
{
    return str2.stringEqual(str1.rawString());
}

inline bool operator != (const Utf8String & str1, const Id & str2)
{
    return !str2.stringEqual(str1.rawString());
}

inline bool operator <  (const Utf8String & str1, const Id & str2)
{
    return !str2.stringGreaterEqual(str1.rawString());
}

inline bool operator == (const Id & str1, const std::string & str2)
{
    return str1.stringEqual(str2);
}

inline bool operator != (const Id & str1, const std::string & str2)
{
    return !str1.stringEqual(str2);
}

inline bool operator <  (const Id & str1, const std::string & str2)
{
    return str1.stringLess(str2);
}

inline bool operator == (const Id & str1, const Utf8String & str2)
{
    return str1.stringEqual(str2.rawString());
}

inline bool operator != (const Id & str1, const Utf8String & str2)
{
    return !str1.stringEqual(str2.rawString());
}

inline bool operator <  (const Id & str1, const Utf8String & str2)
{
    return str1.stringLess(str2.rawString());
}

namespace MLDB {

struct MatrixNamedRow;
struct MatrixRow;
struct MatrixNamedEvent;
struct MatrixEvent;
struct ExpressionValue;
struct RowValueInfo;

/** A row in an expression value is a set of (key, atom, timestamp) pairs. */
typedef std::vector<std::tuple<Id, CellValue, Date> > RowValue;

/** A struct in an expression value is a set of (key, value) pairs. */
typedef std::vector<std::tuple<Id, ExpressionValue> > StructValue;

enum SchemaCompleteness {
    SCHEMA_OPEN,   ///< Schema is open; columns may exist that aren't known
    SCHEMA_CLOSED  ///< Schema is closed; all columns accounted for
};

enum VariableFilter {
    GET_ANY_ONE,  ///< Get any one value; doesn't matter which
    GET_EARLIEST, ///< Get the earliest value only
    GET_LATEST,   ///< Get the latest value only
    GET_ALL       ///< Get all of the values
};

/** This function can be used for optimizations where it is known that
    only one value is possible for a given variable.  It returns whether
    the filter can be ignored for this case.

    Currently, we don't allow GET_ALL to be set anywhere, and so this
    will always return true.
*/
constexpr bool canIgnoreIfExactlyOneValue(VariableFilter) { return true; }

/** How we deal with arrays when parsing JSON into an expression value. */
enum JsonArrayHandling {
    PARSE_ARRAYS, ///< Arrays are parsed into nested expression values
    ENCODE_ARRAYS ///< Arrays are encoded as one-hot or JSON literals
};


/*****************************************************************************/
/* EXPRESSION VALUE INFO                                                     */
/*****************************************************************************/

/** Information and manipulation object that tells us about a the output of
    an expression and allows us to manipulate it.

    The aspects of a cell's value are:
    - Its type
    - Its range
    - The distribution of values that it can have.  For example, a boolean
      variable has a probability of being true and a probability of being
      false.

    Note that each node of an expression tree has a different value info
    node, even if the type of the underlying data is the same, as it may
    have different characteristics such as range, etc.
*/

struct ExpressionValueInfo {
    virtual ~ExpressionValueInfo();

    /// Memory allocated for cell, in bytes
    virtual size_t getCellSize() const = 0;

    /// Default constructor
    virtual void initCell(void * data) const = 0;

    /// Copy constructor, from the same type
    virtual void initCopyFromCell(void * data, void * otherData) const = 0;

    /// Move constructor, from the same type
    virtual void initMoveFromCell(void * data, void * otherData) const = 0;

    /// Destructor
    virtual void destroyCell(void * data) const = 0;

    /// Move assignment
    virtual void moveFromCell(void * data, void * fromData) const = 0;

    /// Copy assignment
    virtual void copyFromCell(void * data, const void * otherData) const = 0;

    /// Is this a scalar, ie just an atomic value.  If this returns false,
    /// then it is a structured type which can be used in different
    /// contexts, for example to generate a row.
    virtual bool isScalar() const = 0;

    /// Is this a value description for a row?
    virtual bool isRow() const;

    /// Could the thing described by this value description return a row?
    virtual bool couldBeRow() const
    {
        return false;
    }

    /// Could the thing described by this value description return a scalar?
    virtual bool couldBeScalar() const
    {
        return true;
    }

    /// Return whether the schema for a row is closed (only those columns are
    /// there) or open (other columns may be present).  Default throws that
    /// it's not a row.
    virtual SchemaCompleteness getSchemaCompleteness() const;

    /// Return the set of known columns for a row.  Default throws that it's not
    /// a row.
    virtual std::vector<KnownColumn> getKnownColumns() const;

    /// Return a list of all known column names
    virtual std::vector<ColumnName> allColumnNames() const;
    
    /// Is the other value compatible with this info?
    virtual bool isCompatible(const ExpressionValue & value) const = 0;

    /// Check that this type can convert to the other type.  Default will
    /// return true (for now).
    virtual bool isConvertibleTo(const ExpressionValueInfo & other) const
    {
        return true;
    }

    /** Return the ExpressionValueInfo that covers the range of types of
        each of the two given values.
    */
    static std::shared_ptr<ExpressionValueInfo>
    getCovering(const std::shared_ptr<ExpressionValueInfo> & info1,
                const std::shared_ptr<ExpressionValueInfo> & info2);
    
    /** Return the ExpressionValueInfo that is equivalent to the two
        others, merged together.  The two inputs must be row
        information
    */
    static std::shared_ptr<RowValueInfo>
    getMerged(const std::shared_ptr<ExpressionValueInfo> & info1,
              const std::shared_ptr<ExpressionValueInfo> & info2);

    /** Return the expression value info for a version of this value
        flattened into a row.  This only needs to be provided if
        isScalar() returns false.

        Default implementation throws an exception.
    */
    virtual std::shared_ptr<RowValueInfo> getFlattenedInfo() const;

    /** Perform the actual flattening of a value.  The given function
        should be called to write values.  This only needs to be
        provided if isScalar() returns false.
        
        Default implementation throws an exception.
    */
    virtual void flatten(const ExpressionValue & value,
                         const std::function<void (const ColumnName & columnName,
                                                   const CellValue & value,
                                                   Date timestamp)> & write)
        const;

    /** Get a description of the subtype if it's a scalar.  Default will
        throw that it's not a scalar.
    */
    virtual std::string getScalarDescription() const;

    /** Get the expression value info of a value nested at any level
        with columns name separated by a '.'
    */
    virtual std::shared_ptr<ExpressionValueInfo> findNestedColumn(
            const Utf8String& variableName,
            SchemaCompleteness& schemaCompleteness)
    {
        schemaCompleteness = SCHEMA_CLOSED;
        return std::shared_ptr<ExpressionValueInfo>();
    }
};

PREDECLARE_VALUE_DESCRIPTION(std::shared_ptr<ExpressionValueInfo>);


/*****************************************************************************/
/* COLUMN SPARSITY                                                           */
/*****************************************************************************/

enum ColumnSparsity {
    COLUMN_IS_DENSE,  ///< Column is mandatory and must be present
    COLUMN_IS_SPARSE  ///< Column is sparse and may or may not be present
};

DECLARE_ENUM_DESCRIPTION(ColumnSparsity);


/*****************************************************************************/
/* KNOWN COLUMN                                                              */
/*****************************************************************************/

/** Describes a column that is known as part of a row. */

struct KnownColumn {
    KnownColumn()
        : sparsity(COLUMN_IS_SPARSE)
    {
    }

    KnownColumn(ColumnName columnName,
                std::shared_ptr<ExpressionValueInfo> valueInfo,
                ColumnSparsity sparsity)
        : columnName(columnName),
          valueInfo(valueInfo),
          sparsity(sparsity)
    {
    }
    
    ColumnName columnName;
    std::shared_ptr<ExpressionValueInfo> valueInfo;
    ColumnSparsity sparsity;
};

DECLARE_STRUCTURE_DESCRIPTION(KnownColumn);


/*****************************************************************************/
/* STORAGE TYPE                                                              */
/*****************************************************************************/

/** Declares the underlying binary type of a tensor's stored data element. */

enum StorageType {
    ST_FLOAT32,
    ST_FLOAT64,
    ST_INT8,
    ST_UINT8,
    ST_INT16,
    ST_UINT16,
    ST_INT32,
    ST_UINT32,
    ST_INT64,
    ST_UINT64,
    ST_BLOB,
    ST_STRING,
    ST_UTF8STRING,
    ST_CELLVALUE,
    ST_BOOL
};

DECLARE_ENUM_DESCRIPTION(StorageType);


/*****************************************************************************/
/* TENSOR METADATA                                                           */
/*****************************************************************************/

/** Used to add metadata and structure to a tensor valued expression. */

struct TensorMetadata {

    /// Name of each dimension
    std::vector<Utf8String> dimNames;
};

DECLARE_STRUCTURE_DESCRIPTION(TensorMetadata);


/*****************************************************************************/
/* EXPRESSION VALUE                                                          */
/*****************************************************************************/

/** This is the type used to hold the value of an expression.  It can be a
    scalar, in which case it is a CellValue.  Or it can be a row, a table
    or a structured value.
*/

struct ExpressionValue {
    typedef std::vector<std::tuple<ColumnName, ExpressionValue> > Row;

    /// Initialize as null.
    ExpressionValue();
    ExpressionValue(std::nullptr_t, Date ts);
    static ExpressionValue null(Date ts);

    /// Initialize as integer
    ExpressionValue(char intValue, Date ts) { initInt(intValue, ts); }
    ExpressionValue(unsigned char intValue, Date ts) { initInt(intValue, ts); }
    ExpressionValue(signed char intValue, Date ts) { initInt(intValue, ts); }
    ExpressionValue(int intValue, Date ts) { initInt(intValue, ts); }
    ExpressionValue(unsigned int intValue, Date ts) { initInt(intValue, ts); }
    ExpressionValue(short int intValue, Date ts) { initInt(intValue, ts); }
    ExpressionValue(unsigned short int intValue, Date ts) { initInt(intValue, ts); }
    ExpressionValue(long int intValue, Date ts) { initInt(intValue, ts); }
    ExpressionValue(unsigned long int intValue, Date ts) { initUInt(intValue, ts); }
    ExpressionValue(long long int intValue, Date ts) { initInt(intValue, ts); }
    ExpressionValue(unsigned long long int intValue, Date ts) { initUInt(intValue, ts); }

    ExpressionValue(double doubleValue, Date ts)
        : type_(NONE)
    {
        initAtom(doubleValue, ts);
    }

    ExpressionValue(float floatValue, Date ts)
        : type_(NONE)
    {
        initAtom(floatValue, ts);
    }

    ExpressionValue(Date val, Date ts);

    // Construct from ASCII string.  These all check the ASCII-ness of the
    // value and will throw if it's not really ASCII.
    ExpressionValue(const std::string & asciiStringValue, Date ts);
    ExpressionValue(const char * asciiStringValue, Date ts);

    // Construct from Unicode string.  These will all convert to Utf8
    ExpressionValue(const std::wstring & unicodeStringValue, Date ts);
    ExpressionValue(const std::basic_string<char16_t> & utf16StringValue, Date ts);
    ExpressionValue(const std::basic_string<char32_t> & utf32StringValue, Date ts);
    ExpressionValue(const Utf8String & unicodeStringValue, Date ts);
    ExpressionValue(const char16_t * utf16StringValue, Date ts);
    ExpressionValue(const char32_t * utf32StringValue, Date ts);

    // Construct from a structure or embedding of simple values with
    // common names.  This is more efficient than a row as only the
    // values are kept in memory; the column names are shared
    ExpressionValue(std::vector<CellValue> values,
                    std::shared_ptr<const std::vector<ColumnName> > cols,
                    Date ts);

    ExpressionValue(std::vector<CellValue> values,
                    Date ts);

    // Construct from an embedding of simple values with common names
    // This is more efficient than a row as only the values are kept
    ExpressionValue(std::vector<float> values,
                    std::shared_ptr<const std::vector<ColumnName> > cols,
                    Date ts);

    // Construct from a pure embedding
    ExpressionValue(std::vector<float> values,
                    Date ts);

    // Construct from a pure embedding
    ExpressionValue(std::vector<double> values,
                    Date ts);
    
    /** Construct from a generalized uniform tensor, which is stored as
        a contiguous (flat), column-major array (ie, standard c storage).
        
        Parameters:
        - ts: the timestamp at which the tensor occurs;
        - data: a shared pointer to the data of the tensor;
        - storage: the data type that's stored in the data
        - dims: a list of dimensions.  Length zero is a single scalar.
          Length 1 is a one-dimensional array with length dims[0].
          And so on.
        - md: metadata about the tensor, to allow us to better understand
          and present what the information is (eg, if it's an image, what
          the channels are).  Optional.

        When accessed as a row, the column names will be like [1,2].
    */
    static ExpressionValue
    tensor(Date ts,
           std::shared_ptr<const void> data,
           StorageType storage,
           std::vector<size_t> dims,
           std::shared_ptr<const TensorMetadata> md = nullptr);
    
    //Construct from a m/d/s time interval
    static ExpressionValue
    fromInterval(uint16_t months, uint16_t days, float seconds, Date ts);

    ExpressionValue(CellValue atom, Date ts) noexcept;
    ExpressionValue(RowValue row) noexcept;

    // Construct from a set of named values as a row
    ExpressionValue(std::vector<std::tuple<Id, ExpressionValue> > vals) noexcept;
    // Construct from JSON.  Will convert to an atom or a row.
    ExpressionValue(const Json::Value & json, Date ts);
    
    /** Construct from a JSON literal string, parsing as we go. */
    static ExpressionValue
    parseJson(JsonParsingContext & context,
              Date timestamp,
              JsonArrayHandling arrays = PARSE_ARRAYS);

    ~ExpressionValue();
    ExpressionValue(const ExpressionValue & other);
    ExpressionValue(ExpressionValue && other) noexcept;

    ExpressionValue & operator = (const ExpressionValue & other);
    ExpressionValue & operator = (ExpressionValue && other) noexcept;

    void swap(ExpressionValue & other) noexcept;

    double toDouble() const;
    int64_t toInt() const;
    
    bool asBool() const;

    bool isTrue() const;

    bool isFalse() const;

    bool empty() const;

    bool isString() const;

    bool isUtf8String() const;

    bool isAsciiString() const;

    bool isInteger() const;

    bool isNumber() const;

    bool isTimestamp() const;

    bool isTimeinterval() const;

    bool isObject() const;

    bool isArray() const;
    
    bool isAtom() const;

    bool isRow() const;

    bool isTensor() const;

    std::string toString() const;

    Utf8String toUtf8String() const;

    std::basic_string<char32_t> toWideString() const;

    Utf8String getTypeAsUtf8String() const;

    const CellValue & getAtom() const;

    /// Destructive getAtom() call, that moves it into the result
    CellValue stealAtom();
    const Row & getRow() const;

    // like getRow, but it actually moves it out so no copying is required
    Row stealRow();

    CellValue coerceToString() const;
    CellValue coerceToInteger() const;
    CellValue coerceToNumber() const;
    CellValue coerceToBoolean() const;
    CellValue coerceToTimestamp() const;
    CellValue coerceToAtom() const;
    CellValue coerceToBlob() const;

    // Return the timestamp at which all of the information in this value
    // was known.  This is used to determine the timestamp of the output
    // of an expression involving this value.
    Date getEffectiveTimestamp() const
    {
        return ts_;
    }

    /// Set the effective timestamp
    void setEffectiveTimestamp(Date ts)
    {
        ts_ = ts;
    }

    /** Return the minimum timestamp represented in the value, ie the
        minimum of all entries.
    */
    Date getMinTimestamp() const;

    /** Return the maximum timestamp represented in the value, ie the
        maximum of all entries.
    */
    Date getMaxTimestamp() const;

    /** return if this value should be sorted as earlier or later than the one provided
    */
    bool isEarlier(const Date& compareTimeStamp, const ExpressionValue& compareValue) const;
    bool isLater(const Date& compareTimeStamp, const ExpressionValue& compareValue) const;

    // Return the given field name.  Valid for anything that is a
    // structured type... rows, JSON values, objects, arrays, embeddings.
    ExpressionValue getField(const Utf8String & fieldName,
                             const VariableFilter & filter = GET_LATEST) const;

    // Return the given field by index.  Valid for anything that is a
    // arrays or embedding.
    ExpressionValue getField(int fieldIndex) const;

    const ExpressionValue* findNestedField(const Utf8String & fieldName,
                                       const VariableFilter & filter = GET_LATEST) const;

    // Return the given field name.  Valid for anything that is a
    // structured type... rows, JSON values, objects, arrays, embeddings.
    ExpressionValue getField(const char * fieldName,
                             const VariableFilter & filter = GET_LATEST) const
    {
        return getField(Utf8String(fieldName), filter);
    }

    // Return the given field name.  Valid for anything that is a
    // structured type... rows, JSON values, objects, arrays, embeddings.
    ExpressionValue getField(const std::string & fieldName,
                             const VariableFilter & filter = GET_LATEST) const
    {
        return getField(Utf8String(fieldName), filter);
    }

    /** Return an embedding from the value, asserting on the length.  If the
        length is -1, it is unknown and any length will be accepted. */
    ML::distribution<float, std::vector<float> >
    getEmbedding(ssize_t knownLength = -1) const;

    /** Return an embedding from the value, asserting on the length.  If the
        length is -1, it is unknown and any length will be accepted. */
    ML::distribution<double, std::vector<double> >
    getEmbeddingDouble(ssize_t knownLength = -1) const;

    /** Return an embedding from the value, asserting on the names of the
        columns.  Note that this method will not extract the given names;
        it will only assert that the names in the value are the same as
        those given in knownNames.

        The numDone parameter tells how many have already been done.  It
        is used as an offset in the knownNames array.
    */
    ML::distribution<float, std::vector<float> >
    getEmbedding(const std::vector<ColumnName> & knownNames,
                 ssize_t maxLength = -1,
                 size_t numDone = 0) const;

    /** Iterate over the child expression. */
    bool forEachSubexpression(const std::function<bool (const Id & columnName,
                                                        const Id & prefix,
                                                        const ExpressionValue & val)>
                                                  & onSubexpression,
                              const Id & prefix = Id()) const;

    /** Iterate over child columns, returning a reference that may be moved
        elsewhere.

        Only works for row-typed values.
    */
    bool forEachColumnDestructive
        (const std::function<bool (Id & columnName, ExpressionValue & val)>
         & onSubexpression) const;

    /** Iterate over the flattened representation. */
    bool forEachAtom(const std::function<bool (const Id & columnName,
                                               const Id & prefix,
                                               const CellValue & val,
                                               Date ts) > & onAtom,
                     const Id & columnName = Id()) const;

    /** For a row (structured) storage, returns the number of elements
        that are in it.  Note that this is the non-flattened version,
        ie the number of times forEachColumnDestructive will be called.
    */
    size_t rowLength() const;
    
    /** Write a flattened representation of the current value to the given
        dataset row or event.
    */
    void appendToRow(const Id & columnName, MatrixNamedRow & row) const;
    void appendToRow(const Id & columnName, RowValue & row) const;
    void appendToRow(const Id & columnName, StructValue & row) const;

    /** Write a flattened representation of the current value to the given
        dataset row or event, moving values and destroying this object in
        the process.
    */
    void appendToRowDestructive(Id & columnName, RowValue & row);
    void appendToRowDestructive(Id & columnName, StructValue & row);

    /// Destructively merge into the given row
    void mergeToRowDestructive(RowValue & row);

    /// Destructively merge into the given row
    void mergeToRowDestructive(StructValue & row);

    /** Apply filter to select values in the row according to their timestamp */
    Row getFiltered(const VariableFilter & filter = GET_LATEST) const;

    /// Return if it is a row, and contains the given key
    std::pair<bool, Date> hasKey(const Utf8String & key) const;

    /// Return if it is a row, and one of the elements is the given value,
    /// treating it like a set
    std::pair<bool, Date> hasValue(const ExpressionValue & value) const;

    int compare(const ExpressionValue & other) const;

    bool operator == (const ExpressionValue & other) const;
    bool operator != (const ExpressionValue & other) const
    {
        return ! operator == (other);
    }
    bool operator <  (const ExpressionValue & other) const;

    bool operator <= (const ExpressionValue & other) const
    {
        return ! other.operator < (*this);
    }
    
    bool operator >  (const ExpressionValue & other) const
    {
        return other.operator < (*this);
    }
    
    bool operator >= (const ExpressionValue & other) const
    {
        return !operator < (other);
    }

    /** Return the most specialized possible value info for this given value.
        Used for static analysis of constants.
    */
    std::shared_ptr<ExpressionValueInfo> getSpecializedValueInfo() const;

    /** Print a JSON representation of the pure value (ignoring timestamps).
        This is how to round-trip JSON through the ExpressionValue class.

        If a key has multiple values, only one will be kept.
    */
    void extractJson(JsonPrintingContext & context) const;

    /** Same, but return the JSON directly. */
    Json::Value extractJson() const;

    /** Return a hash of the value.  Note the the timestamps are NOT and MUST
        NOT BE incorporated into the calculated value.
    */
    size_t hash() const;

private:
    void initInt(int64_t intValue, Date ts);
    void initUInt(uint64_t intValue, Date ts);
    void initAtom(CellValue value, Date ts) noexcept
    {
        ExcAssertEqual(type_, NONE);
        ts_ = ts;
        if (value.empty())
            return;
        new (storage_) CellValue(std::move(value));
        type_ = ATOM;
    }
    void initRow(Row row) noexcept;
    void initRow(std::shared_ptr<const Row> row) noexcept;

    void setAtom(CellValue value, Date ts);

    /** Same as forEachColumnDestructive, but templated on the function
        type to allow for inlining.  Defined in expression_value.cc.
    */
    template<typename Fn>
    bool forEachColumnDestructiveT(Fn && onSubexpression) const;

    enum Type {
        NONE,     ///< Expression is empty or not initialized yet.  Shouldn't be exposed to user.
        ATOM,     ///< Expression is an atom (CellValue), including null
        ROW,      ///< Expression is a row, ie a destructured complex type with independent timestamps
        STRUCT,   ///< Expression is a structure of keys and elements.
        TENSOR    ///< Uniform typed n-dimensional array of atoms
    };

    Type type_;

    /// This is how we store a structure with a single value for each
    /// element and an external set of column names
    struct Struct;

    /// This is how we store a tensor, which is a dense array of a
    /// uniform data type.
    struct Tensor;

    /// This is where the underlying values are actually stored
    union {
        uint64_t storage_[2];
        CellValue cell_;
        std::shared_ptr<const Row> row_;
        std::shared_ptr<const Struct> struct_;
        std::shared_ptr<const Tensor> tensor_;
    };
    Date ts_;   ///< Nominal timestamp that the information was known

    friend class ExpressionValueDescription;
    friend class ExpressionValueDescriptionNoTimestamp;
};


std::ostream & operator << (std::ostream & stream, const ExpressionValue & val);

PREDECLARE_VALUE_DESCRIPTION(ExpressionValue);

/** Create a value description for expression values, that prints them
    out without timestamps.
*/
std::shared_ptr<ValueDescriptionT<ExpressionValue> >
getExpressionValueDescriptionNoTimestamp();


/*****************************************************************************/
/* EXPRESSION VALUE INFO TEMPLATE                                            */
/*****************************************************************************/

/** Expression value for a cell of a given type. */
template<typename Storage>
struct ExpressionValueInfoT: public ExpressionValueInfo {
    virtual ~ExpressionValueInfoT();

    virtual size_t getCellSize() const;

    /// Default constructor
    virtual void initCell(void * data) const;

    /// Copy constructor, from the same type
    virtual void initCopyFromCell(void * data, void * otherData) const;

    /// Move constructor, from the same type
    virtual void initMoveFromCell(void * data, void * otherData) const;

    /// Destructor
    virtual void destroyCell(void * data) const;

    /// Move assignment
    virtual void moveFromCell(void * data, void * fromData) const;

    /// Copy assignment
    virtual void copyFromCell(void * data, const void * fromData) const;
};

template<typename Storage>
struct ScalarExpressionValueInfoT: public ExpressionValueInfoT<Storage> {

    virtual bool isScalar() const
    {
        return true;
    }

    /// Is the other value compatible with this info?
    virtual bool isCompatible(const ExpressionValue & value) const
    {
        // TODO: tighter check
        return value.isAtom() || value.empty();
    }

    virtual std::string getScalarDescription() const
    {
        return ML::type_name<Storage>();
    }
};

extern template class ExpressionValueInfoT<float>;
extern template class ExpressionValueInfoT<double>;
extern template class ExpressionValueInfoT<CellValue>;
extern template class ExpressionValueInfoT<std::string>;
extern template class ExpressionValueInfoT<Utf8String>;
extern template class ExpressionValueInfoT<std::vector<unsigned char> >;
extern template class ExpressionValueInfoT<int64_t>;
extern template class ExpressionValueInfoT<uint64_t>;
extern template class ExpressionValueInfoT<char>;
extern template class ExpressionValueInfoT<Date>;
extern template class ScalarExpressionValueInfoT<float>;
extern template class ScalarExpressionValueInfoT<double>;
extern template class ScalarExpressionValueInfoT<CellValue>;
extern template class ScalarExpressionValueInfoT<std::string>;
extern template class ScalarExpressionValueInfoT<Utf8String>;
extern template class ScalarExpressionValueInfoT<std::vector<unsigned char> >;
extern template class ScalarExpressionValueInfoT<int64_t>;
extern template class ScalarExpressionValueInfoT<uint64_t>;
extern template class ScalarExpressionValueInfoT<char>;
extern template class ScalarExpressionValueInfoT<Date>;

extern template class ExpressionValueInfoT<RowValue>;
extern template class ExpressionValueInfoT<ExpressionValue>;
extern template class ExpressionValueInfoT<ML::distribution<double, std::vector<double> > >;


/*****************************************************************************/
/* EXPRESSION VALUE INFO INSTANTIATIONS                                      */
/*****************************************************************************/

/** Value that is always empty.  Doesn't require any storage to be allocated
    since there is nothing to store.
*/
struct EmptyValueInfo: public ExpressionValueInfo {
    virtual ~EmptyValueInfo();
    virtual size_t getCellSize() const;
    virtual void initCell(void * data) const;
    virtual void initCopyFromCell(void * data, void * otherData) const;
    virtual void initMoveFromCell(void * data, void * otherData) const;
    virtual void destroyCell(void * data) const;
    virtual void moveFromCell(void * data, void * fromData) const;
    virtual void copyFromCell(void * data, const void * fromData) const;

    virtual bool isScalar() const;

    virtual bool isCompatible(const ExpressionValue & value) const
    {
        return value.empty();
    }
};

struct AtomValueInfo: public ScalarExpressionValueInfoT<CellValue> {
};

struct NumericValueInfo: public ScalarExpressionValueInfoT<double> {
};

struct StringValueInfo: public ScalarExpressionValueInfoT<std::string> {
};

struct Utf8StringValueInfo: public ScalarExpressionValueInfoT<Utf8String> {
};

struct BlobValueInfo: public ScalarExpressionValueInfoT<std::vector<unsigned char> > {
    /// Is the other value compatible with this info?
    virtual bool isCompatible(const ExpressionValue & value) const
    {
        return value.isAtom() && value.getAtom().isBlob();
    }

    virtual std::string getScalarDescription() const
    {
        return "blob";
    }
};

struct BooleanValueInfo: public ScalarExpressionValueInfoT<char> {
};

struct IntegerValueInfo: public ScalarExpressionValueInfoT<int64_t> {
};

struct Uint64ValueInfo: public ScalarExpressionValueInfoT<uint64_t> {
};

struct Float32ValueInfo: public ScalarExpressionValueInfoT<float> {
};

struct Float64ValueInfo: public ScalarExpressionValueInfoT<double> {
};

struct TimestampValueInfo: public ScalarExpressionValueInfoT<Date> {
};

/// May be anything
struct AnyValueInfo: public ExpressionValueInfoT<ExpressionValue> {

    AnyValueInfo();

    virtual bool isScalar() const;

    virtual std::shared_ptr<RowValueInfo> getFlattenedInfo() const;

    virtual void flatten(const ExpressionValue & value,
                         const std::function<void (const ColumnName & columnName,
                                                   const CellValue & value,
                                                   Date timestamp)> & write)
        const;

    virtual bool isCompatible(const ExpressionValue & value) const
    {
        return true;
    }

    virtual SchemaCompleteness getSchemaCompleteness() const;

    virtual std::vector<KnownColumn> getKnownColumns() const;

    virtual bool couldBeRow() const
    {
        return true;
    }

    virtual bool couldBeScalar() const
    {
        return true;
    }
};

/// For an embedding
struct EmbeddingValueInfo: public ExpressionValueInfoT<ML::distribution<double, std::vector<double> > > {
    EmbeddingValueInfo(ssize_t numDims);

    ssize_t numDims;

    virtual bool isScalar() const;

    virtual std::shared_ptr<RowValueInfo> getFlattenedInfo() const;

    virtual void flatten(const ExpressionValue & value,
                         const std::function<void (const ColumnName & columnName,
                                                   const CellValue & value,
                                                   Date timestamp)> & write)
        const;

    virtual bool isCompatible(const ExpressionValue & value) const
    {
        return value.isArray();
    }
};

/// For a row.  This may have information about columns within that row.
struct RowValueInfo: public ExpressionValueInfoT<RowValue> {
    
    RowValueInfo(const std::vector<KnownColumn> & columns,
                 SchemaCompleteness completeness = SCHEMA_CLOSED);

    virtual bool isScalar() const;

    virtual std::shared_ptr<RowValueInfo> getFlattenedInfo() const;

    virtual void flatten(const ExpressionValue & value,
                         const std::function<void (const ColumnName & columnName,
                                                   const CellValue & value,
                                                   Date timestamp)> & write) const;

    virtual std::shared_ptr<ExpressionValueInfo> findNestedColumn(
            const Utf8String& variableName,
            SchemaCompleteness& schemaCompleteness);

    virtual std::vector<KnownColumn> getKnownColumns() const;
    virtual SchemaCompleteness getSchemaCompleteness() const;

    std::vector<KnownColumn> columns;
    SchemaCompleteness completeness;

    virtual bool isCompatible(const ExpressionValue & value) const
    {
        return value.isRow();
    }

    virtual bool isRow() const
    {
        return true;
    }

    virtual bool couldBeRow() const
    {
        return true;
    }

    virtual bool couldBeScalar() const
    {
        return false;
    }
};

/// For a row.  This may have information about columns within that row.
struct UnknownRowValueInfo: public RowValueInfo {
    
    UnknownRowValueInfo()
        : RowValueInfo({}, SCHEMA_OPEN)
    {
    }

    virtual bool isRow() const
    {
        return true;
    }
};

// Get a value description for expression values
ValueDescriptionT<std::shared_ptr<ExpressionValueInfo> > *
getDefaultDescription(std::shared_ptr<ExpressionValueInfo> *);
ValueDescriptionT<std::shared_ptr<ExpressionValueInfo> > *
getDefaultDescriptionUninitialized(std::shared_ptr<ExpressionValueInfo> *);


/*****************************************************************************/
/* NAMED ROW VALUE                                                           */
/*****************************************************************************/

/** Return value of a row expression, including row name. */

struct NamedRowValue {
    RowName rowName;
    RowHash rowHash;
    StructValue columns;

    //operator MatrixNamedRow() const;
    MatrixNamedRow flattenDestructive();
};


DECLARE_STRUCTURE_DESCRIPTION(NamedRowValue);



/*****************************************************************************/
/* SEARCH ROW FUNCTIONS                                                      */
/*****************************************************************************/

/** These functions search the given row for the named value. */

const ExpressionValue *
searchRow(const std::vector<std::tuple<ColumnHash, CellValue, Date> > & columns,
          const ColumnName & key,
          const VariableFilter & filter,
          ExpressionValue & storage);

const ExpressionValue *
searchRow(const std::vector<std::tuple<ColumnName, CellValue, Date> > & columns,
          const ColumnHash & key,
          const VariableFilter & filter,
          ExpressionValue & storage);

const ExpressionValue *
searchRow(const std::vector<std::tuple<ColumnName, CellValue, Date> > & columns,
          const ColumnName & key,
          const VariableFilter & filter,
          ExpressionValue & storage);

const ExpressionValue *
searchRow(const std::vector<std::tuple<ColumnName, ExpressionValue> > & columns,
          const ColumnName & key,
          const VariableFilter & filter,
          ExpressionValue & storage);


} // namespace MLDB
} // namespace Datacratic

// Allow std::unordered_xxx<ExpressionValue> to work
namespace std {

template<typename T> struct hash;

template<>
struct hash<Datacratic::MLDB::ExpressionValue> : public std::unary_function<Datacratic::MLDB::ExpressionValue, size_t>
{
    size_t operator()(const Datacratic::MLDB::ExpressionValue & val) const { return val.hash(); }
};

} // namespace std
