/** sparse_matrix.h                                                -*- C++ -*-
    Jeremy Barnes, 8 April 2015
    Copyright (c) 2015 mldb.ai inc.  All rights reserved.

    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

    Dataset object for storing data in MLDB.
*/

#pragma once

#include "mldb/utils/compact_vector.h"
#include "mldb/types/value_description_fwd.h"


namespace MLDB {

/** Implementation of a base matrix.  This stores rows (indexed by integers)
    of data points, each with a column (integer), timestamp (64 bit integer),
    value (encoded integer up to 64 bits) and zero or more metadata items
    (binary blob of variable length).

    This is the basic unit of storage for data in the sparse matrix format.
*/

struct BaseEntry {
    BaseEntry(uint64_t rowcol = 0,
              uint64_t timestamp = 0,
              uint64_t val = 0,
              uint64_t tag = 0,
              compact_vector<std::string, 0, uint32_t> metadata
                = compact_vector<std::string, 0, uint32_t>())
        : rowcol(rowcol),
          timestamp(timestamp),
          val(val),
          tag(tag),
          metadata(std::move(metadata))
    {
    }

    BaseEntry(BaseEntry && other) noexcept
        : rowcol(other.rowcol),
          timestamp(other.timestamp),
          val(other.val),
          tag(other.tag),
          metadata(std::move(other.metadata))
    {
    }

    BaseEntry(const BaseEntry & other) = default;

    BaseEntry & operator = (BaseEntry && other) noexcept
    {
        rowcol = other.rowcol;
        timestamp = other.timestamp;
        val = other.val;
        tag = other.tag;
        metadata = std::move(other.metadata);
        return *this;
    }

    BaseEntry & operator = (const BaseEntry & other) = default;

    uint64_t rowcol;
    uint64_t timestamp;
    uint64_t val;
    uint32_t tag;
    compact_vector<std::string, 0, uint32_t> metadata;
};

struct MatrixWriteTransaction;

struct MatrixReadTransaction {

    struct Stream {       

        virtual std::shared_ptr<MatrixReadTransaction::Stream> clone() const = 0;

        virtual void initAt(size_t start) = 0;

        virtual uint64_t next() = 0;

        virtual uint64_t current() const = 0;
    };

    virtual ~MatrixReadTransaction()
    {
    }

    virtual bool iterateRow(uint64_t rowNum,
                            const std::function<bool (const BaseEntry & entry)> & onEntry) = 0;
    virtual bool iterateRows(const std::function<bool (uint64_t row)> & onRow) = 0;
    virtual bool knownRow(uint64_t rowNum) = 0;

    virtual size_t rowCount() const = 0;

    virtual std::shared_ptr<MatrixReadTransaction::Stream> getStream() const = 0;

    virtual std::shared_ptr<MatrixWriteTransaction> startWriteTransaction() const = 0;

    virtual bool isSingleReadEntry() const = 0;
};

struct MatrixWriteTransaction: virtual public MatrixReadTransaction {
    virtual ~MatrixWriteTransaction()
    {
    }

    virtual void recordRow(uint64_t rowNum, const BaseEntry * entries, int n) = 0;
    virtual void recordCol(uint64_t colNum, const BaseEntry * entries, int n) = 0;
    virtual void recordRow(uint64_t rowNum, BaseEntry * entries, int n) = 0;
    virtual void recordCol(uint64_t colNum, BaseEntry * entries, int n) = 0;

    virtual bool commitNeedsThread() const = 0;
    virtual void commit() = 0;

    virtual bool isSingleReadEntry() const {return false;}
};

DECLARE_STRUCTURE_DESCRIPTION(BaseEntry);

struct BaseMatrix {
    virtual ~BaseMatrix()
    {
    }

    virtual std::shared_ptr<MatrixReadTransaction> startReadTransaction() const = 0;
    virtual std::shared_ptr<MatrixWriteTransaction> startWriteTransaction() = 0;
    virtual void optimize() = 0;
};

} // namespace MLDB

