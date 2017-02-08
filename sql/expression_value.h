/** expression_value.h                                             -*- C++ -*-
    Jeremy Barnes, 14 February 2015

    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

    Code for the type that holds the value of an expression.
*/

#pragma once

#include "dataset_fwd.h"
#include "path.h"
#include "mldb/types/value_description_fwd.h"
#include "mldb/types/date.h"
#include "mldb/arch/demangle.h"
#include "mldb/base/exc_assert.h"
#include "mldb/utils/compact_vector.h"
#include "cell_value.h"
#include <cstdint>

// NOTE TO MLDB DEVELOPERS: This is an API header file.  No includes
// should be added, especially value_description.h.



namespace MLDB {

template<typename T, typename Underlying>
struct distribution;

struct Date;
struct MatrixNamedRow;
struct MatrixRow;
struct MatrixNamedEvent;
struct MatrixEvent;
struct ExpressionValue;
struct ExpressionValueInfo;
struct RowValueInfo;
struct EmbeddingValueInfo;

/** A row in an expression value is a set of (key, atom, timestamp) pairs. */
typedef std::vector<std::tuple<Path, CellValue, Date> > RowValue;

/** A struct in an expression value is a set of (key, value) pairs. */
typedef std::vector<std::tuple<PathElement, ExpressionValue> > StructValue;

/** Return the ValueInfo that corresponds to the given
    ValueDescription.
*/
std::shared_ptr<ExpressionValueInfo>
valueInfoFromDescription(const std::shared_ptr<const ValueDescription> & value);

/** Return the RowValueInfo that corresponds to the given
    ValueDescription, which must be a StructureValueDescription.
*/
std::shared_ptr<RowValueInfo>
rowInfoFromDescription(const std::shared_ptr<const ValueDescription> & value);

template<typename T>
std::shared_ptr<ExpressionValueInfo>
valueInfoForType()
{
    return valueInfoFromDescription(getDefaultDescriptionSharedT<T>());
}

template<typename T>
std::shared_ptr<RowValueInfo>
rowInfoForType()
{
    return rowInfoFromDescription(getDefaultDescriptionSharedT<T>());
}

/** Return the RowValueInfo that corresponds to the given
    ValueDescription, which must be a StructureValueDescription.
*/
std::shared_ptr<RowValueInfo>
valueInfoForStructure(const ValueDescription & desc);


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

DECLARE_ENUM_DESCRIPTION(JsonArrayHandling);

/** Vector of dimensions for an embedding. */
typedef compact_vector<size_t, 4> DimsVector;

/*****************************************************************************/
/* STORAGE TYPE                                                              */
/*****************************************************************************/

/** Declares the underlying binary type of a embedding's stored data element. */

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
    ST_ATOM,
    ST_BOOL,
    ST_TIMESTAMP,
    ST_TIMEINTERVAL
};

DECLARE_ENUM_DESCRIPTION(StorageType);

// Usage: GetStorageType<T>::val is the storage type to store
// that kind of value.
template<typename T>
struct GetStorageType;

#define SPECIALIZE_STORAGE_TYPE(type, st) \
template<> struct GetStorageType<type> { static constexpr StorageType val = st; };

SPECIALIZE_STORAGE_TYPE(float,                ST_FLOAT32);
SPECIALIZE_STORAGE_TYPE(double,               ST_FLOAT64);
SPECIALIZE_STORAGE_TYPE(unsigned char,        ST_UINT8);
SPECIALIZE_STORAGE_TYPE(signed char,          ST_INT8);
SPECIALIZE_STORAGE_TYPE(char,                 ST_UINT8);
SPECIALIZE_STORAGE_TYPE(signed short,         ST_INT16);
SPECIALIZE_STORAGE_TYPE(unsigned short,       ST_UINT16);
SPECIALIZE_STORAGE_TYPE(signed int,           ST_INT32);
SPECIALIZE_STORAGE_TYPE(unsigned int,         ST_UINT32);
SPECIALIZE_STORAGE_TYPE(signed long,          ST_INT64);
SPECIALIZE_STORAGE_TYPE(unsigned long,        ST_UINT64);
SPECIALIZE_STORAGE_TYPE(signed long long,     ST_INT64);
SPECIALIZE_STORAGE_TYPE(unsigned long long,   ST_UINT64);
SPECIALIZE_STORAGE_TYPE(std::vector<uint8_t>, ST_BLOB);
SPECIALIZE_STORAGE_TYPE(std::string,          ST_STRING);
SPECIALIZE_STORAGE_TYPE(Utf8String,           ST_UTF8STRING);
SPECIALIZE_STORAGE_TYPE(CellValue,            ST_ATOM);
SPECIALIZE_STORAGE_TYPE(bool,                 ST_BOOL);
SPECIALIZE_STORAGE_TYPE(Date,                 ST_TIMESTAMP);
SPECIALIZE_STORAGE_TYPE(Path,                 ST_ATOM);

/** Return the ExpressionValueInfo type for the given storage type. */
std::shared_ptr<ExpressionValueInfo>
getValueInfoForStorage(StorageType type);

/** Return the minimum storage type for a given CellValue. */
StorageType valueStorageType(const CellValue & val);
StorageType valueStorageType(const ExpressionValue & val);

/** Return a storage type that can store both. */
StorageType coveringStorageType(StorageType type1, StorageType type2);
StorageType coveringStorageType(StorageType type1, const CellValue & val2);
StorageType coveringStorageType(StorageType type1, const ExpressionValue & val2);

/** How many bytes is a single instance of the given storage type? */
size_t sizeofStorageType(StorageType type);

/** Allocate a buffer of storage of the given type.  The elements will be
    initialized with their default constructor.
*/
std::shared_ptr<void>
allocateStorageBuffer(size_t size, StorageType storage);

std::shared_ptr<void>
allocateStorageBuffer(const DimsVector & size, StorageType storage);

/** Return the size of a storage buffer of the given type.  Includes only
    direct (no indirect) storage.
*/
uint64_t storageBufferBytes(size_t size, StorageType storage);

uint64_t storageBufferBytes(const DimsVector & size, StorageType storage);

/** Copy part of a storage buffer into another.  The elements in to must be
    initialized already.
*/
void copyStorageBuffer(const void * from, size_t fromOffset, StorageType fromType,
                       void * to, size_t toOffset, StorageType toType,
                       size_t numElements);

/** Move part of a storage buffer into another.  If the elements that were
    moved from from are accessed afterwards, it's undefined what the result
    will be, but they still must be destroyed.


*/
void moveStorageBuffer(void * from, size_t fromOffset, StorageType fromType,
                       void * to, size_t toOffset, StorageType toType,
                       size_t numElements);

/** Fill in the given range of elements of the storage buffer with the
    contents of the given CellValue.  The elements should already be
    initialized.
*/
void fillStorageBuffer(void * buffer, size_t offset, StorageType storageType,
                       size_t numElements, const CellValue & val);

/** Fill in the given range of elements of the storage buffer with the
    contents of the given CellValue.  The elements should not already be
    initialized.
*/
void initializeStorageBuffer(void * buffer, StorageType storageType,
                             size_t numElements, const CellValue & val);

/** Fill in the given range of elements of the storage buffer with the
    default constructor.
*/
void initializeStorageBuffer(void * buffer, StorageType storageType,
                             size_t numElements);


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

    /// Is this a value description for a row?  This includes a normal row
    /// and a superposition.
    virtual bool isRow() const;

    static std::shared_ptr<RowValueInfo>
    toRow(std::shared_ptr<ExpressionValueInfo> row);

    /// Is this a value description for an embedding?
    virtual bool isEmbedding() const;

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

    /// Could the thing described by this value description return an embedding?
    virtual bool couldBeEmbedding() const 
    { 
        return false; 
    }

    /// Return whether the schema for a row is closed (only those columns are
    /// there) or open (other columns may be present).  Default throws that
    /// it's not a row.
    virtual SchemaCompleteness getSchemaCompleteness() const;

    /// Return whether the schema for a row is closed (only those columns are
    /// there) or open (other columns may be present).  Will return closed
    /// for atoms, and open for rows with any open schema inside them at any
    /// recursive depth.
    virtual SchemaCompleteness getSchemaCompletenessRecursive() const;

    /// Return the set of known columns for a row.  Default throws that it's not
    /// a row.
    virtual std::vector<KnownColumn> getKnownColumns() const;

    /// Return the set of known atoms for a row.  Default throws that it's not
    /// a row.
    virtual std::vector<KnownColumn> getKnownAtoms(const ColumnPath prefix = ColumnPath()) const;

    /// Return a list of all known column names
    virtual std::vector<ColumnPath> allColumnNames() const;

    virtual std::vector<ColumnPath> allAtomNames() const;
    
    /// Is the other value compatible with this info?
    virtual bool isCompatible(const ExpressionValue & value) const = 0;

    /// Check that this type can convert to the other type.  Default will
    /// return true (for now).
    virtual bool isConvertibleTo(const ExpressionValueInfo & other) const
    {
        return true;
    }

    // Does it always returns the same value
    virtual bool isConst() const = 0;

    //return an expression value info of the same type, with the assigned constness
    virtual std::shared_ptr<ExpressionValueInfo> getConst(bool constant) const = 0;

    /** Return the ExpressionValueInfo that covers the range of types of
        each of the two given values.
    */
    static std::shared_ptr<ExpressionValueInfo>
    getCovering(const std::shared_ptr<ExpressionValueInfo> & info1,
                const std::shared_ptr<ExpressionValueInfo> & info2);

    /// Function type used to merge together two value information
    /// objects.
    typedef std::function<std::shared_ptr<ExpressionValueInfo>
                          (const ColumnPath & columnName,
                           const std::shared_ptr<ExpressionValueInfo> & lhs,
                           const std::shared_ptr<ExpressionValueInfo> & rhs)>
    MergeColumnInfo;
    
    /** Return the ExpressionValueInfo that is equivalent to the two
        others, merged together.  The two inputs must be row
        information.

        The mergeInfo function will be used to determine the value
        information for two columns that are merged together.  If one
        of the rows is not present, a null pointer will be passed.
        The default will take lhs if present, otherwise rhs.
    */
    static std::shared_ptr<RowValueInfo>
    getMerged(const std::shared_ptr<ExpressionValueInfo> & info1,
              const std::shared_ptr<ExpressionValueInfo> & info2,
              MergeColumnInfo mergeColumnInfo = nullptr);

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
                         const std::function<void (const ColumnPath & columnName,
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
    virtual std::shared_ptr<ExpressionValueInfo>
    findNestedColumn(const ColumnPath& columnName) const;

    /** Return the info object for the given column.  Default will throw that
        the column is unknown; row info needs to override.
    */
    virtual std::shared_ptr<ExpressionValueInfo>
    getColumn(const PathElement & columnName) const;

    /** Return the shape of an embedding.  For scalars, it's the empty
        vector.  For vectors, matrices, tensors it's the real shape.

        If it can't be converted to an embedding, it will throw.
    */
    virtual std::vector<ssize_t> getEmbeddingShape() const;

    /** Return the data type of an embedding.  If it's not an embedding,
        it will throw.
    */
    virtual StorageType getEmbeddingType() const;

    typedef distribution<double, std::vector<double> > DoubleDist;

    /** Return a function which, when called, will extract an embedding
        from the given value.
    */
    typedef std::function<DoubleDist (const ExpressionValue &)>
    ExtractDoubleEmbeddingFunction;

    /** Return a function that extracts the given embedding, in the order
        of columns provided.  This will throw if the column names are not
        compatible with those given.
    */
    virtual ExtractDoubleEmbeddingFunction
    extractDoubleEmbedding(const std::vector<ColumnPath> & cols) const;

    /** Function used to turn two expression values into compatible
        embeddings.  By compatible, we mean that there are the same
        number of columns and they occur in the same order in each.

        This is returned from getCompatibleDoubleEmbeddings().
    */
    typedef std::function<std::tuple<DoubleDist,
                                     DoubleDist,
                                     std::shared_ptr<const void>,
                                     Date>
                          (const ExpressionValue & exp1,
                           const ExpressionValue & exp2)>
    GetCompatibleDoubleEmbeddingsFn;

    /** Function used to convert an embedding back into an expression
        value object that's compatible with it.  It's used where we
        want to perform operations on embeddings and then turn them
        back into an ExpressionValue that contains the original column
        names and structure.
        
        This is returned from getCompatibleDoubleEmbeddings().
    */
    typedef std::function<ExpressionValue (std::vector<double> vals,
                                           const std::shared_ptr<const void> & info,
                                           Date timestamp) >
    ReconstituteFromDoubleEmbeddingFn;

    /** Returns a function that can be called to extract a compatible
        embedding from two values, as well as a function to convert it
        back to the original representation, and a value info object
        used to describe the output of the reconstitution.

        Used when binding a function that needs to extract a compatible
        embedding from two values.

        These functions will work both statically (where the columns are
        known ahead of time, so they can be extracted directly via a
        specialized function) and dynamically (where the columns aren't
        known ahead of time, and so need to be discovered and aligned
        on each call).

        Default will call an override either from this or other, and if
        neither is found will extract row names (statically or dynamically)
        and work from there.
    */
    virtual std::tuple<GetCompatibleDoubleEmbeddingsFn,
                       std::shared_ptr<ExpressionValueInfo>,
                       ReconstituteFromDoubleEmbeddingFn>
    getCompatibleDoubleEmbeddings(const ExpressionValueInfo & other) const;
};

PREDECLARE_VALUE_DESCRIPTION(std::shared_ptr<ExpressionValueInfo>);
PREDECLARE_VALUE_DESCRIPTION(std::shared_ptr<RowValueInfo>);


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
        : sparsity(COLUMN_IS_SPARSE), offset(VARIABLE_OFFSET)
    {
    }

    KnownColumn(ColumnPath columnName,
                std::shared_ptr<ExpressionValueInfo> valueInfo,
                ColumnSparsity sparsity,
                int32_t offset = VARIABLE_OFFSET)
        : columnName(columnName),
          valueInfo(valueInfo),
          sparsity(sparsity),
          offset(offset)
    {
    }

    bool operator < (const KnownColumn& other) const {
        return columnName < other.columnName;
    }
    
    ColumnPath columnName;
    std::shared_ptr<ExpressionValueInfo> valueInfo;

    /// Tells is whether we can count on the column being there or not.  The
    /// COLUMN_IS_DENSE property is basically required for offset to not be
    /// VARIABLE_OFFSET.
    ColumnSparsity sparsity;

    /// If this column has a fixed position in the expression value returned
    /// then its offset is set here.  Otherwise, it has VARIABLE_OFFSET which
    /// meansthat calling code can't rely on the position its sorted in.
    int32_t offset;

    /// Allows us to say that we don't know the position of a column in an
    /// expression.
    static constexpr int32_t VARIABLE_OFFSET = -1;
};

DECLARE_STRUCTURE_DESCRIPTION(KnownColumn);


/*****************************************************************************/
/* EMBEDDING METADATA                                                        */
/*****************************************************************************/

/** Used to add metadata and structure to a embedding valued expression. */

struct EmbeddingMetadata {

    /// Name of each dimension
    std::vector<Utf8String> dimNames;
};

DECLARE_STRUCTURE_DESCRIPTION(EmbeddingMetadata);


/*****************************************************************************/
/* EXPRESSION VALUE                                                          */
/*****************************************************************************/

/** This is the type used to hold the value of an expression.  It can be a
    scalar, in which case it is a CellValue.  Or it can be a row, a table
    or a structured value.

    IMPORTANT
    
    This class implements storage and manipulation of values in the MLDB
    SQL system.  However, there are two aspects of those which should be
    understood separately:

    1.  The logical type of a value, which specifies what operations are
        available and what the effect of those operations is.
    2.  The actual storage type of a value, which allows for different
        ways of storing things to improve efficiency.  The storage type
        of a value should NEVER affect which operations are available,
        and the same logical value should ALWAYS return the same result
        from an operation NO MATTER HOW IT IS STORED.

    The logical types are the most important, so we will start with those.
    A value in MLDB is either:

    - An atomic value, with a timestamp.  These are handled by the
      CellValue class for the value part.  These always have exactly one
      way to store them: inside a CellValue, or not at all for a null.
    - A row, which is logically a set of named columns, each of which has a
      value (which may be an atom or a row) and a timestamp.  Since each of
      those values may also be a row, this representation allows for recursive
      structures.

      There are certain sub-types of rows that meet certain conditions.
      These still meet the definition of a row, however, and should not
      behave differently for standard operations:

      - A row with consecutive columns from 0 to n, and where each element
        has the same timestamp, is an *array* of length n+1
      - A row with n distinct columns (with any name), where each has
        one value and the same timestamp, is an *object*.  (This definition
        is rarely used, but it's important as it describes the subset of
        values that can be losslessly converted to JSON).  Note that all
        arrays are also objects.
      - A row with consecutive columns from 0 to n, each of which is an
        atomic value, with a common type and a common timestamp across
        all elements, is a one dimensional embedding of shape [n+1].
      - A row with consecutive columns from 0 to n and 0 to m, named
        like 0.0, 0.1, ..., 0.n, 1.0, ..., 1.n, ..., m.0, ..., m.n,
        each of which is an atomic value, with a common type and with a
        common timestamp across all elements, is a 2 dimensional embedding
        of shape [m+1, n+1].
    
    Storage types exist because it would be to expensive to always store
    all of the elements as per the definition.  Instead, sometimes these
    may be stored more efficiently, with the ExpressionValue class handling
    translation of operations on the logical API to operate efficiently
    on the storage class.

    The three ways of storing rows are:

    1.  As a structured representation, which is a sequence of (name, value)
        pairs where names are simple strings (ie, not paths: PathElement not Path)
        and each value can also be structured;
    2.  As a flattened representation, which is a sequence of (path, atom,
        timestamp) tuples.  Here, the paths may represent a whole route
        through an object, like a.b.c.  This is how most datasets represent
        their data, but logically operations should have the same result
        no matter how they are stored.
    3.  As an embedding representation, which has a contiguous array of
        atomic values and a shape to understand how the indexes apply, along
        with a single timestamp that is shared amongst all elements.  This
        is an efficient way to store numerical data like indexes or matrices.

    Again, although it is possible to ask the ExpressionValue if its elements
    are an embedding or stored in a structured or flattened representation,
    this is only to allow for optimizations and the result of an operation
    should be identical no matter how it is stored.  In particular, the user
    is not required to know that a row with keys 0 and 1 and an integer in
    each cell is actually an embedding, and to store it as one.
*/

struct ExpressionValue {
    typedef StructValue Structured;

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
        : type_(Type::NONE)
    {
        initAtom(doubleValue, ts);
    }

    ExpressionValue(float floatValue, Date ts)
        : type_(Type::NONE)
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
                    std::shared_ptr<const std::vector<ColumnPath> > cols,
                    Date ts);

    // Construct from an embedding of simple values with common names
    // This is more efficient than a row as only the values are kept
    ExpressionValue(const std::vector<double> & values,
                    std::shared_ptr<const std::vector<ColumnPath> > cols,
                    Date ts);

    /** Construct from an embedding, with the given values.  If
        dims is not passed or empty, it will be set to a vector
        of the length of values.
    */
    ExpressionValue(std::vector<CellValue> values,
                    Date ts,
                    DimsVector shape = DimsVector());

    // Construct from a pure embedding
    ExpressionValue(std::vector<float> values,
                    Date ts,
                    DimsVector shape = DimsVector());

    // Construct from a pure embedding
    ExpressionValue(std::vector<double> values,
                    Date ts,
                    DimsVector shape = DimsVector());

    // Construct from a pure embedding
    ExpressionValue(std::vector<int> values,
                    Date ts,
                    DimsVector shape = DimsVector());
    
    /** Construct from a generalized uniform embedding, which is stored as
        a contiguous (flat), column-major array (ie, standard c storage).
        
        Parameters:
        - ts: the timestamp at which the embedding occurs;
        - data: a shared pointer to the data of the embedding;
        - storage: the data type that's stored in the data
        - dims: a list of dimensions.  Length zero is a single scalar.
          Length 1 is a one-dimensional array with length dims[0].
          And so on.
        - md: metadata about the embedding, to allow us to better understand
          and present what the information is (eg, if it's an image, what
          the channels are).  Optional.

        When accessed as a row, the column names will be like [1,2].
    */
    static ExpressionValue
    embedding(Date ts,
              std::shared_ptr<const void> data,
              StorageType storage,
              DimsVector dims,
              std::shared_ptr<const EmbeddingMetadata> md = nullptr);

    /** Create a single ExpressionValue that is the superposition of several
        values.

        This will merge together anything that is non-conflicting, and
        provide a superposition of the rest.

        Note that multiple atomic values will be represented in a row with
        empty keys.
    */
    static ExpressionValue superpose(std::vector<ExpressionValue> vals);

    //Construct from a m/d/s time interval
    static ExpressionValue
    fromInterval(uint16_t months, uint16_t days, float seconds, Date ts);

    ExpressionValue(CellValue atom, Date ts) noexcept;
    ExpressionValue(RowValue row) noexcept;

    enum Sorting {
        SORTED,
        MAY_BE_SORTED,
        NOT_SORTED
    };

    enum Duplicates {
        NO_DUPLICATES,
        MAY_HAVE_DUPLICATES,
        HAS_DUPLICATES
    };

    // Construct from a set of named values as a row
    ExpressionValue(StructValue vals,
                    Sorting sorting = MAY_BE_SORTED,
                    Duplicates duplicates = MAY_HAVE_DUPLICATES) noexcept;

    // Construct from JSON.  Will convert to an atom or a row.
    ExpressionValue(const Json::Value & json, Date ts);
    
    /** Construct from a JSON literal string, parsing as we go. */
    static ExpressionValue
    parseJson(JsonParsingContext & context,
              Date timestamp,
              JsonArrayHandling arrays = PARSE_ARRAYS);

    ~ExpressionValue()
    {
        if (type_ == Type::NONE)
            return;
        destroy();
    }

    ExpressionValue(const ExpressionValue & other);

    MLDB_ALWAYS_INLINE ExpressionValue(ExpressionValue && other) noexcept
    {
        // Dodgy as hell.  But none of them have self referential pointers, and so it
        // works and with no possibility of an exception.
        std::memcpy(this, &other, sizeof(ExpressionValue));
        other.type_ = Type::NONE;
    }

    ExpressionValue & operator = (const ExpressionValue & other);

    MLDB_ALWAYS_INLINE ExpressionValue &
    operator = (ExpressionValue && other) noexcept
    {
        ExpressionValue newMe(std::move(other));
        swap(newMe);
        return *this;
    }

    MLDB_ALWAYS_INLINE
    void swap(ExpressionValue & other) noexcept
    {
        // Dodgy as hell.  But none of them have self referential pointers, and so it
        // works and with no possibility of an exception.
        std::swap(type_,    other.type_);
        std::swap(storage_[0], other.storage_[0]);
        std::swap(storage_[1], other.storage_[1]);
        std::swap(ts_, other.ts_);
    }

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

#if 0
    bool isObject() const;
#endif    

    bool isArray() const;

    bool isAtom() const;

    bool isRow() const;

    // isNull() -> use empty()

    /// Is this a superposition?  Meaning that there are multiple values
    /// superposed.
    bool isSuperposition() const;

    bool isEmbedding() const;

    std::string toString() const;

    Utf8String toUtf8String() const;

    std::basic_string<char32_t> toWideString() const;

    std::string getTypeAsString() const;

    const CellValue & getAtom() const;

    /// Destructive getAtom() call, that moves it into the result
    CellValue stealAtom();

    CellValue coerceToString() const;
    CellValue coerceToInteger() const;
    CellValue coerceToNumber() const;
    CellValue coerceToBoolean() const;
    CellValue coerceToTimestamp() const;
    CellValue coerceToAtom() const;
    CellValue coerceToBlob() const;

    /** Return the current value, but with non-embedding representations
        converted to a uniform embedding representation.  This should
        give exactly the same result from any expression as the original
        version but may be significantly faster.
    */
    ExpressionValue coerceToEmbedding() const;

    /** Convert the current ExpressionValue to a path (used as a column or
        row name).
        
        This handles integers, strings (both converted to length one
        paths) and arrays (converted to structured arrays).
    */
    Path coerceToPath() const;

    // Return the timestamp at which all of the information in this value
    // was known.  This is used to determine the timestamp of the outputcellva
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

    /** Return if this value should be sorted as earlier or later than the one
        provided.
    */
    bool isEarlier(const Date& compareTimeStamp,
                   const ExpressionValue& compareValue) const;
    bool isLater(const Date& compareTimeStamp,
                 const ExpressionValue& compareValue) const;

    // Return the value of the given column (non-nested).
    ExpressionValue getColumn(const PathElement & columnName,
                              const VariableFilter & filter = GET_LATEST) const;

    const ExpressionValue *
    tryGetColumn(const PathElement & columnName,
                 ExpressionValue & storage,
                 const VariableFilter & filter = GET_LATEST) const;

    // Return the given nested column.  Valid for anything that is a
    // row type... rows, JSON values, objects, arrays, embeddings.
    ExpressionValue getNestedColumn(const ColumnPath & columnName,
                                    const VariableFilter & filter = GET_LATEST) const;

    const ExpressionValue *
    tryGetNestedColumn(const ColumnPath & columnName,
                       ExpressionValue & storage,
                       const VariableFilter & filter = GET_LATEST) const;

    // Return true if the nested column exist, false otherwise
    bool hasNestedColumn(const Path & column) const;

    /** Return an embedding from the value, asserting on the length.  If the
        length is -1, it is unknown and any length will be accepted. */
    distribution<float, std::vector<float> >
    getEmbedding(ssize_t knownLength = -1) const;

    /** Return an embedding from the value, asserting on the length.  If the
        length is -1, it is unknown and any length will be accepted. */
    distribution<double, std::vector<double> >
    getEmbeddingDouble(ssize_t knownLength = -1) const;

    /** Return a flattened embedding as CellValues. */
    std::vector<CellValue>
    getEmbeddingCell(ssize_t knownLength = -1) const;

    /** Return the shape of the embedding. */
    DimsVector
    getEmbeddingShape() const;

    /** Reshape the embedding into a new shape.  The total number of
        elements must be equal to the new total number of elements.

        This will throw an exception if the shape doesn't match.
    */
    ExpressionValue reshape(DimsVector newShape) const;

    /** Reshape the embedding into a new shape.  Any new elements
        will be initialized with newVal.  Any extra elements
        will be removed.
    */
    ExpressionValue reshape(DimsVector newShape,
                            const ExpressionValue & newVal) const;

    /** Return an embedding from the value, asserting on the names of the
        columns.  Note that this method will not extract the given names;
        it will only assert that the names in the value are the same as
        those given in knownNames.

        The numDone parameter tells how many have already been done.  It
        is used as an offset in the knownNames array.
    */
    distribution<double, std::vector<double> >
    getEmbedding(const ColumnPath * knownNames, size_t len) const;

    /** Convert an embedding to an array of the given storage type.  The
        elements will be filled in to the existing memory.  Throws an
        exception if len is not correct to cover the exact amount of
        memory required.
    */
    void convertEmbedding(void * buf, size_t len, StorageType bufType) const;

    /** Return the storage type of the embedding
        Will throw an error if not an embedding.
    */
    StorageType getEmbeddingType() const;

    /** Iterate over the child expression, with an ExpressionValue at each
        level.  Note that if isRow() is false, than this function will
        NOT call the callback; it's only called for row-valued values.

        This function:
        - Must be called once only for each columnName
        - May have an empty columnName, when we have a superposition
          (multiple atom values at the current level)
        - Will be called in a single thread only
        - Will not be called again if it returns false
        
        The returned value is the output of the last time the callback was
        called, or true if never called.  In other words, if a callback
        returns false the return value is false, otherwise true.
    */
    bool forEachColumn(const std::function<bool (const PathElement & columnName,
                                                 const ExpressionValue & val)>
                       & onColumn) const;
    
    /** Iterate over child columns, returning a reference that may be moved
        elsewhere.  This is similar to forEachColumn(), but will allow the
        values returned to be moved out of place, and will leave *this in
        an indeterminate state afterwards.

        Only works for row-typed values.
    */
    bool forEachColumnDestructive
        (const std::function<bool (PathElement & columnName, ExpressionValue & val)>
         & onColumn) const;

    /** Iterate over the flattened representation. */
    bool forEachAtom(const std::function<bool (const Path & columnName,
                                               const Path & prefix,
                                               const CellValue & val,
                                               Date ts) > & onAtom,
                     const Path & columnName = Path()) const;


    /** Iterate over the flattened representation, destroying this object
        as we go to make the operations more efficient.  The called object
        will be left in an indeterminate state afterwards.
    */
    bool forEachAtomDestructive(const std::function<bool (Path & columnName,
                                                          CellValue & val,
                                                          Date ts) > & onAtom);


    /** Iterate over each superposed value.  This occurs only when there is more
        than one value in the ExpressionValue, for example when it has different
        values at different points in time.

        If it's not a superposition, then only the one value is returned.
    */
    bool forEachSuperposedValue
        (const std::function<bool (const ExpressionValue & val)> & onValue) const;

    /** For a row (structured) storage, returns the number of elements
        that are in it.  Note that this is the non-flattened version,
        ie the number of times forEachColumnDestructive will be called.
    */
    size_t rowLength() const;
    
    /** Write a flattened representation of the current value to the given
        dataset row or event, prepending the given column name.
    */
    void appendToRow(const Path & columnName, MatrixNamedRow & row) const;
    void appendToRow(const Path & columnName, RowValue & row) const;
    void appendToRow(const Path & columnName, StructValue & row) const;

    /** Write a flattened representation of the current value to the given
        dataset row or event, moving values and destroying this object in
        the process.
    */
    void appendToRowDestructive(ColumnPath & columnName, RowValue & row);
    void appendToRowDestructive(const Path & columnName, StructValue & row);

    /// Destructively merge into the given row
    void mergeToRowDestructive(RowValue & row);

    /// Destructively merge into the given row
    void mergeToRowDestructive(StructValue & row);

    /** Apply filter to select values in the row according to their timestamp */
    const ExpressionValue &
    getFiltered(const VariableFilter & filter,
                ExpressionValue & storage) const;

    /** Apply filter to select values in the row according to their timestamp.
        This will leave the current object in an indeterminate state
        afterwards.
    */
    ExpressionValue getFilteredDestructive(const VariableFilter & filter);

    /** Returns the number of times that forEachAtom() will return a
        value to the callback.  In other words, the total number of
        distinct atoms in this object.  Atoms will return one.
    */
    size_t getAtomCount() const;

    /** Returns the number of unique column names that forEachAtom()
        will return.  In other words, the total number of distinct
        elements in the flattened representation of the object.
        Atoms will return one.
    */
    size_t getUniqueAtomCount() const;
    
    typedef std::function<bool (const ColumnPath & columnName,
                                std::pair<CellValue, Date> * vals1,
                                std::pair<CellValue, Date> * vals2,
                                size_t n1,
                                size_t n2)> OnMatchingColumn;

    enum Outer {
        INNER,
        OUTER_LEFT,
        OUTER_RIGHT,
        OUTER
    };

    /** Join of two rows on column names.  Calls back the function with
        each column and the values from each side.
    */
    static bool joinColumns(const ExpressionValue & val1,
                            const ExpressionValue & val2,
                            const OnMatchingColumn & onMatchingColumn,
                            Outer outer);

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
    std::shared_ptr<ExpressionValueInfo> getSpecializedValueInfo(bool constant) const;

    /** Print a JSON representation of the pure value (ignoring timestamps).
        This is how to round-trip JSON through the ExpressionValue class.

        If a key has multiple values, only one will be kept.
    */
    void extractJson(JsonPrintingContext & context) const;

    /** Same, but return the JSON directly. */
    Json::Value extractJson() const;

    /** Convert to the given type described by its ValueDescription. */
    template<typename T>
    T extractT(const ValueDescription & desc
               = *getDefaultDescriptionSharedT<T>()) const
    {
        T result;
        extractImpl(&result, desc);
        return result;
    }

    /** Return a hash of the value.  Note the the timestamps are NOT and MUST
        NOT BE incorporated into the calculated value.
    */
    size_t hash() const;

    void DebugPrint();

private:
    void extractImpl(void * obj, const ValueDescription & desc) const;

    void initInt(int64_t intValue, Date ts);
    void initUInt(uint64_t intValue, Date ts);
    void initAtom(CellValue value, Date ts) noexcept
    {
        //ExcAssertEqual((int)type_, (int)Type::NONE);
        ts_ = ts;
        if (value.empty())
            return;
        new (storage_) CellValue(std::move(value));
        type_ = Type::ATOM;
    }
    void initStructured(Structured row) noexcept;
    void initStructured(Structured value, bool needsSorting, bool hasDuplicates) noexcept;
    void initStructured(std::shared_ptr<const Structured> row) noexcept;
    const Structured & getStructured() const;

    void setAtom(CellValue value, Date ts);

    /** Same as forEachColumnDestructive, but templated on the function
        type to allow for inlining.  Defined in expression_value.cc.
    */
    template<typename Fn>
    bool forEachColumnDestructiveT(Fn && onColumn) const;

    /** Same as forEachAtomDestructive, but templated on the function
        type to allow for inlining.  Defined in expression_value.cc.
    */
    template<typename Fn>
    bool forEachAtomDestructiveT(Fn && onColumn);

    /** Destroy complex value, leaving an empty value. */
    void destroy();

    enum class Type : uint8_t {
        NONE,        ///< Expression is empty or not initialized yet.  Shouldn't be exposed to user.
        ATOM,        ///< Expression is an atom (CellValue), including null
        STRUCTURED,  ///< Expression is a structured, ie a destructured complex type with independent timestamps
        EMBEDDING,    ///< Uniform typed n-dimensional array of atoms
        SUPERPOSITION ///< Multiple values of the same thing
    };

    Type type_;

    static std::string print(Type t);
    void assertType(Type requested, const std::string & details="") const;

    /// This is how we store a structure with a single value for each
    /// element and an external set of column names
    struct Flattened;

    /// This is how we store a embedding, which is a dense array of a
    /// uniform data type.
    struct Embedding;

    /// This is how we store superposed values, which is multiple values
    /// of the same variable, possibly at different time points
    struct Superposition;

    /// This is where the underlying values are actually stored
    union {
        uint64_t storage_[2];
        CellValue cell_;
        std::shared_ptr<const Structured> structured_;
        std::shared_ptr<const Flattened> flattened_;
        std::shared_ptr<const Embedding> embedding_;
        std::shared_ptr<const Superposition> superposition_;
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

/** Create an expression value description specialized to the given type.
    This can be used to extract the specialized info for a type.
*/
std::shared_ptr<const ValueDescriptionT<ExpressionValue> >
makeExpressionValueDescription(std::shared_ptr<ExpressionValueInfo> info);

/** Create an expression value description specialized to the given type.
    This can be used to extract the specialized info for a type.

    It will take ownership of the object in info.
*/
std::shared_ptr<const ValueDescriptionT<ExpressionValue> >
makeExpressionValueDescription(ExpressionValueInfo * info);

/** Get the expression value description from this value description.  If
    it's not an expression value, returns a null pointer.
*/
std::shared_ptr<ExpressionValueInfo>
extractExpressionValueInfo(const std::shared_ptr<const ValueDescription> & desc);


/*****************************************************************************/
/* EXPRESSION VALUE INFO TEMPLATE                                            */
/*****************************************************************************/

/** Expression value for a cell of a given type. */
template<typename Storage>
struct ExpressionValueInfoT: public ExpressionValueInfo {

    ExpressionValueInfoT(bool isConstant) : isConstant(isConstant) {}

    // GCC 5.3 has trouble when this is not inlined
    virtual ~ExpressionValueInfoT()
    {
    }

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

    virtual bool isConst() const { return isConstant; }

protected:

    bool isConstant;
};

template<typename Storage>
struct ScalarExpressionValueInfoT: public ExpressionValueInfoT<Storage> {

    ScalarExpressionValueInfoT() : ExpressionValueInfoT<Storage>(false) {}

    ScalarExpressionValueInfoT(bool isConstant) : ExpressionValueInfoT<Storage>(isConstant) {}

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
        return MLDB::type_name<Storage>();
    }

    virtual std::vector<ssize_t> getEmbeddingShape() const
    {
        return {};
    }

    virtual StorageType getEmbeddingType() const
    {
        // TODO: fix this to depend on the type
        return GetStorageType<Storage>::val;
    }
};

extern template class ExpressionValueInfoT<float>;
extern template class ExpressionValueInfoT<double>;
extern template class ExpressionValueInfoT<CellValue>;
extern template class ExpressionValueInfoT<std::string>;
extern template class ExpressionValueInfoT<Utf8String>;
extern template class ExpressionValueInfoT<std::vector<uint8_t> >;
extern template class ExpressionValueInfoT<Path>;
extern template class ExpressionValueInfoT<int64_t>;
extern template class ExpressionValueInfoT<uint64_t>;
extern template class ExpressionValueInfoT<char>;
extern template class ExpressionValueInfoT<Date>;
extern template class ScalarExpressionValueInfoT<float>;
extern template class ScalarExpressionValueInfoT<double>;
extern template class ScalarExpressionValueInfoT<CellValue>;
extern template class ScalarExpressionValueInfoT<std::string>;
extern template class ScalarExpressionValueInfoT<Utf8String>;
extern template class ScalarExpressionValueInfoT<std::vector<uint8_t> >;
extern template class ScalarExpressionValueInfoT<Path>;
extern template class ScalarExpressionValueInfoT<int64_t>;
extern template class ScalarExpressionValueInfoT<uint64_t>;
extern template class ScalarExpressionValueInfoT<char>;
extern template class ScalarExpressionValueInfoT<Date>;

extern template class ExpressionValueInfoT<RowValue>;
extern template class ExpressionValueInfoT<ExpressionValue>;
extern template class ExpressionValueInfoT<distribution<double, std::vector<double> > >;


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
    virtual std::string getScalarDescription() const;

    virtual bool isCompatible(const ExpressionValue & value) const
    {
        return value.empty();
    }

    virtual bool isConst() const { return true; }

    virtual std::shared_ptr<ExpressionValueInfo> getConst(bool constant) const {
        return std::make_shared<EmptyValueInfo>(); //ignore it
    }
};

struct AtomValueInfo: public ScalarExpressionValueInfoT<CellValue> {
    AtomValueInfo() : ScalarExpressionValueInfoT<CellValue>() {}
    AtomValueInfo(bool isconst) : ScalarExpressionValueInfoT<CellValue>(isconst) {}
    virtual std::shared_ptr<ExpressionValueInfo> getConst(bool constant) const {
        return std::make_shared<AtomValueInfo>(constant);
    }
};

struct NumericValueInfo: public ScalarExpressionValueInfoT<double> {
    NumericValueInfo() : ScalarExpressionValueInfoT<double>() {}
    NumericValueInfo(bool isconst) : ScalarExpressionValueInfoT<double>(isconst) {}
    virtual std::shared_ptr<ExpressionValueInfo> getConst(bool constant) const {
        return std::make_shared<NumericValueInfo>(constant);
    }
};

struct StringValueInfo: public ScalarExpressionValueInfoT<std::string> {
    StringValueInfo() : ScalarExpressionValueInfoT<std::string>() {}
    StringValueInfo(bool isconst) : ScalarExpressionValueInfoT<std::string>(isconst) {}
    virtual std::shared_ptr<ExpressionValueInfo> getConst(bool constant) const {
        return std::make_shared<StringValueInfo>(constant);
    }
};

struct Utf8StringValueInfo: public ScalarExpressionValueInfoT<Utf8String> {
    Utf8StringValueInfo() : ScalarExpressionValueInfoT<Utf8String>() {}
    Utf8StringValueInfo(bool isconst) : ScalarExpressionValueInfoT<Utf8String>(isconst) {}
    virtual std::shared_ptr<ExpressionValueInfo> getConst(bool constant) const {
        return std::make_shared<Utf8StringValueInfo>(constant);
    }
};

struct BlobValueInfo: public ScalarExpressionValueInfoT<std::vector<uint8_t> > {

    BlobValueInfo() : ScalarExpressionValueInfoT<std::vector<uint8_t> >() {}
    BlobValueInfo(bool isconst) : ScalarExpressionValueInfoT<std::vector<uint8_t> >(isconst) {}

    /// Is the other value compatible with this info?
    virtual bool isCompatible(const ExpressionValue & value) const
    {
        return value.isAtom() && value.getAtom().isBlob();
    }

    virtual std::string getScalarDescription() const
    {
        return "blob";
    }

     virtual std::shared_ptr<ExpressionValueInfo> getConst(bool constant) const {
        return std::make_shared<BlobValueInfo>(constant);
    }
};

struct PathValueInfo: public ScalarExpressionValueInfoT<Path> {

    PathValueInfo() : ScalarExpressionValueInfoT<Path>() {}
    PathValueInfo(bool isconst) : ScalarExpressionValueInfoT<Path>(isconst) {}

    /// Is the other value compatible with this info?
    virtual bool isCompatible(const ExpressionValue & value) const
    {
        return value.isAtom() && value.getAtom().isPath();
    }

    virtual std::string getScalarDescription() const
    {
        return "path";
    }

    virtual std::shared_ptr<ExpressionValueInfo> getConst(bool constant) const {
        return std::make_shared<PathValueInfo>(constant);
    }
};

struct BooleanValueInfo: public ScalarExpressionValueInfoT<char> {
    BooleanValueInfo() : ScalarExpressionValueInfoT<char>() {}
    BooleanValueInfo(bool isconst) : ScalarExpressionValueInfoT<char>(isconst) {}
    virtual std::shared_ptr<ExpressionValueInfo> getConst(bool constant) const {
        return std::make_shared<BooleanValueInfo>(constant);
    }
};

struct IntegerValueInfo: public ScalarExpressionValueInfoT<int64_t> {
    IntegerValueInfo() : ScalarExpressionValueInfoT<int64_t>() {}
    IntegerValueInfo(bool isconst) : ScalarExpressionValueInfoT<int64_t>(isconst) {}
    virtual std::shared_ptr<ExpressionValueInfo> getConst(bool constant) const {
        return std::make_shared<IntegerValueInfo>(constant);
    }
};

struct Uint64ValueInfo: public ScalarExpressionValueInfoT<uint64_t> {
    Uint64ValueInfo() : ScalarExpressionValueInfoT<uint64_t>() {}
    Uint64ValueInfo(bool isconst) : ScalarExpressionValueInfoT<uint64_t>(isconst) {}
    virtual std::shared_ptr<ExpressionValueInfo> getConst(bool constant) const {
        return std::make_shared<Uint64ValueInfo>(constant);
    }
};

struct Float32ValueInfo: public ScalarExpressionValueInfoT<float> {
    Float32ValueInfo() : ScalarExpressionValueInfoT<float>() {}
    Float32ValueInfo(bool isconst) : ScalarExpressionValueInfoT<float>(isconst) {}
    virtual std::shared_ptr<ExpressionValueInfo> getConst(bool constant) const {
        return std::make_shared<Float32ValueInfo>(constant);
    }
};

struct Float64ValueInfo: public ScalarExpressionValueInfoT<double> {
    Float64ValueInfo() : ScalarExpressionValueInfoT<double>() {}
    Float64ValueInfo(bool isconst) : ScalarExpressionValueInfoT<double>(isconst) {}
    virtual std::shared_ptr<ExpressionValueInfo> getConst(bool constant) const {
        return std::make_shared<Float64ValueInfo>(constant);
    }
};

struct TimestampValueInfo: public ScalarExpressionValueInfoT<Date> {
    TimestampValueInfo() : ScalarExpressionValueInfoT<Date>() {}
    TimestampValueInfo(bool isconst) : ScalarExpressionValueInfoT<Date>(isconst) {}
    virtual std::shared_ptr<ExpressionValueInfo> getConst(bool constant) const {
        return std::make_shared<TimestampValueInfo>(constant);
    }
};

/// May be anything
struct AnyValueInfo: public ExpressionValueInfoT<ExpressionValue> {

    AnyValueInfo();
    AnyValueInfo(bool constant);

    virtual bool isScalar() const override;

    virtual std::shared_ptr<RowValueInfo> getFlattenedInfo() const override;

    virtual void flatten(const ExpressionValue & value,
                         const std::function<void (const ColumnPath & columnName,
                                                   const CellValue & value,
                                                   Date timestamp)> & write)
        const override;

    virtual bool isCompatible(const ExpressionValue & value) const override
    {
        return true;
    }

    virtual SchemaCompleteness getSchemaCompleteness() const override;

    virtual SchemaCompleteness getSchemaCompletenessRecursive() const override;

    virtual std::vector<KnownColumn> getKnownColumns() const override;

    virtual bool couldBeRow() const override
    {
        return true;
    }

    virtual bool couldBeScalar() const override
    {
        return true;
    }

    virtual bool couldBeEmbedding() const override 
    { 
        return true; 
    }

    virtual std::shared_ptr<ExpressionValueInfo> getConst(bool constant) const override
    {
        return std::make_shared<AnyValueInfo>(constant);
    }
};

/// For a row.  This may have information about columns within that row.
struct RowValueInfo: public ExpressionValueInfoT<RowValue> {
    
    RowValueInfo(const std::vector<KnownColumn> & columns,
                 SchemaCompleteness completeness = SCHEMA_CLOSED,
                 bool isconst = false);

    virtual bool isScalar() const override;

    virtual std::shared_ptr<RowValueInfo> getFlattenedInfo() const override;

    virtual void flatten(const ExpressionValue & value,
                         const std::function<void (const ColumnPath & columnName,
                                                   const CellValue & value,
                                                   Date timestamp)> & write) const override;

    virtual std::shared_ptr<ExpressionValueInfo>
    getColumn(const PathElement & columnName) const override;

    virtual std::shared_ptr<ExpressionValueInfo> 
    findNestedColumn(const ColumnPath& columnName) const override;

    virtual std::vector<KnownColumn> getKnownColumns() const override;
    virtual SchemaCompleteness getSchemaCompleteness() const override;
    virtual SchemaCompleteness getSchemaCompletenessRecursive() const override;

    virtual bool isCompatible(const ExpressionValue & value) const override
    {
        return value.isRow();
    }

    virtual bool isRow() const override
    {
        return true;
    }

    virtual bool couldBeEmbedding() const override 
    { 
        return true; 
    }

    virtual bool couldBeRow() const override
    {
        return true;
    }

    virtual bool couldBeScalar() const override
    {
        return false;
    }

    virtual std::shared_ptr<ExpressionValueInfo>
    getConst(bool constant) const override
    {
        auto clone = std::make_shared<RowValueInfo>(*this);
        clone->isConstant = constant;
        return clone;
    }

protected:
    RowValueInfo() : ExpressionValueInfoT<RowValue>(false)
    {
    }

    std::vector<KnownColumn> columns;
    SchemaCompleteness completeness;
    SchemaCompleteness completenessRecursive;
};

/// For a row where we don't know the columns
struct UnknownRowValueInfo: public RowValueInfo {
    
    UnknownRowValueInfo()
        : RowValueInfo({}, SCHEMA_OPEN)
    {
    }

    virtual bool isRow() const
    {
        return true;
    }

    virtual bool isConst() const { return false; }

    virtual std::shared_ptr<ExpressionValueInfo> getConst(bool constant) const {
        return std::make_shared<UnknownRowValueInfo>(); //ignore it
    }
};

/// For an embedding
struct EmbeddingValueInfo: public RowValueInfo {
    EmbeddingValueInfo(StorageType storageType = ST_ATOM)
        : EmbeddingValueInfo(std::vector<ssize_t>({-1}), storageType)
    {
    }

    EmbeddingValueInfo(ssize_t numDimsForOneDimensionalArray,
                       StorageType storageType = ST_ATOM)
        : shape(1, numDimsForOneDimensionalArray), storageType(storageType)
    {
    }

    EmbeddingValueInfo(std::vector<ssize_t> shape,
                       StorageType storageType = ST_ATOM,
                       bool isConst = false);

    /** Infer the output type for an array of elements of types given in the
        input. */
    EmbeddingValueInfo(const std::vector<std::shared_ptr<ExpressionValueInfo> > & input, bool isConst = false);

    static std::shared_ptr<EmbeddingValueInfo>
    fromShape(const DimsVector& shape, StorageType storageType = ST_ATOM);

    std::vector<ssize_t> shape;
    StorageType storageType;

    /** Return the number of dimensions in the embedding.  This is
        always equal to shape.size().
    */
    virtual size_t numDimensions() const;

    virtual bool isScalar() const override;

    virtual bool isRow() const override;

    virtual bool couldBeRow() const override;

    virtual bool couldBeScalar() const override;

    virtual bool couldBeEmbedding() const override { return true; }

    virtual bool isEmbedding() const override;

    virtual std::vector<ssize_t> getEmbeddingShape() const override;

    virtual StorageType getEmbeddingType() const override;

    virtual std::shared_ptr<RowValueInfo> getFlattenedInfo() const override;

    virtual SchemaCompleteness getSchemaCompleteness() const override;

    virtual SchemaCompleteness getSchemaCompletenessRecursive() const override;

    virtual std::vector<KnownColumn> getKnownColumns() const override;

    virtual std::vector<ColumnPath> allColumnNames() const override;

    virtual void flatten(const ExpressionValue & value,
                         const std::function<void (const ColumnPath & columnName,
                                                   const CellValue & value,
                                                   Date timestamp)> & write)
        const override;

    virtual bool isCompatible(const ExpressionValue & value) const override
    {
        return value.isArray();
    }

    virtual std::shared_ptr<ExpressionValueInfo>
    getConst(bool constant) const override
    {
        auto clone = std::make_shared<EmbeddingValueInfo>(*this);
        clone->isConstant = constant;
        return clone;
    }
};

/*****************************************************************************/
/* Variant Expression Value Info                                             */
/*****************************************************************************/

/** Expression Value info when we dont know which of two value info we will get 
    With a Case for Example.
*/

struct VariantExpressionValueInfo: public ExpressionValueInfoT<ExpressionValue> {

    VariantExpressionValueInfo(std::shared_ptr<ExpressionValueInfo> left, std::shared_ptr<ExpressionValueInfo> right, bool isConst = false);

    //Will return a VariantExpressionValueInfo or another type if the two input type info match in some way.
    static std::shared_ptr<ExpressionValueInfo>
    createVariantValueInfo(std::shared_ptr<ExpressionValueInfo> left, std::shared_ptr<ExpressionValueInfo> right);

    virtual bool isScalar() const override;

    virtual std::shared_ptr<RowValueInfo> getFlattenedInfo() const  override;

    virtual void flatten(const ExpressionValue & value,
                         const std::function<void (const ColumnPath & columnName,
                                                   const CellValue & value,
                                                   Date timestamp)> & write) const override;

    virtual bool isCompatible(const ExpressionValue & value) const override;

    virtual SchemaCompleteness getSchemaCompleteness() const override;

    virtual SchemaCompleteness getSchemaCompletenessRecursive() const  override;

    virtual std::vector<KnownColumn> getKnownColumns() const override;

    virtual bool couldBeRow() const override;

    virtual bool couldBeScalar() const override;

    virtual std::string getScalarDescription() const override;

    virtual std::shared_ptr<ExpressionValueInfo>
    getConst(bool constant) const override
    {
        return std::make_shared<VariantExpressionValueInfo>(left_, right_, constant);
    }

    std::shared_ptr<ExpressionValueInfo> left_;
    std::shared_ptr<ExpressionValueInfo> right_;
};


/*****************************************************************************/
/* NAMED ROW VALUE                                                           */
/*****************************************************************************/

/** Return value of a row expression, including row name. */

struct NamedRowValue {
    RowPath rowName;
    RowHash rowHash;
    StructValue columns;

    //operator MatrixNamedRow() const;
    MatrixNamedRow flattenDestructive();
    MatrixNamedRow flatten() const;
};


DECLARE_STRUCTURE_DESCRIPTION(NamedRowValue);



/*****************************************************************************/
/* SEARCH ROW FUNCTIONS                                                      */
/*****************************************************************************/

/** These functions search the given row for the named value. */
const ExpressionValue *
searchRow(const RowValue & columns,
          const ColumnPath & columnName,
          const VariableFilter & filter,
          ExpressionValue & storage);

const ExpressionValue *
searchRow(const StructValue & columns,
          const PathElement & columnName,
          const VariableFilter & filter,
          ExpressionValue & storage);

const ExpressionValue *
searchRow(const StructValue & columns,
          const ColumnPath & columnName,
          const VariableFilter & filter,
          ExpressionValue & storage);


} // namespace MLDB

// Allow std::unordered_xxx<ExpressionValue> to work
namespace std {

template<typename T> struct hash;

template<>
struct hash<MLDB::ExpressionValue> : public std::unary_function<MLDB::ExpressionValue, size_t>
{
    size_t operator()(const MLDB::ExpressionValue & val) const { return val.hash(); }
};

} // namespace std
