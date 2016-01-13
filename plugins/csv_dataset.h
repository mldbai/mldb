// This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

/** csv_dataset.h                                           -*- C++ -*-
    Jeremy Barnes, 16 December 2014
    Copyright (c) 2014 Datacratic Inc.  All rights reserved.

*/

#include "mldb/core/dataset.h"
#include "mldb/sql/sql_expression.h"

#pragma once

namespace Datacratic {
namespace MLDB {


struct CsvDatasetConfig: public PersistentDatasetConfig {
    CsvDatasetConfig()
        : delimiter(","),
          quoter("\""),
          encoding("utf-8"),
          replaceInvalidCharactersWith(""),
          limit(-1),
          offset(0),
          ignoreBadLines(false),
          select(SelectExpression::STAR),
          where(SqlExpression::TRUE),
          named(SqlExpression::parse("lineNumber()")),
          timestamp(SqlExpression::parse("fileTimestamp()"))
    {
    }

    std::vector<Utf8String> headers;
    std::string delimiter;
    std::string quoter;
    std::string encoding;
    Utf8String replaceInvalidCharactersWith;
    int64_t limit;
    int64_t offset;
    bool ignoreBadLines;

    SelectExpression select;               ///< What to select from the CSV
    std::shared_ptr<SqlExpression> where;  ///< Filter for the CSV
    std::shared_ptr<SqlExpression> named;  ///< Row name to output
    std::shared_ptr<SqlExpression> timestamp;   ///< Timestamp for row

};

DECLARE_STRUCTURE_DESCRIPTION(CsvDatasetConfig);


/*****************************************************************************/
/* CSV DATASET                                                               */
/*****************************************************************************/

/** Dataset type that reads a CSV file.
 */

struct CsvDataset: public Dataset {

    CsvDataset(MldbServer * owner,
               PolyConfig config,
               const std::function<bool (const Json::Value &)> & onProgress);
    
    virtual ~CsvDataset();
    
    virtual Any getStatus() const;

    virtual std::shared_ptr<MatrixView> getMatrixView() const;

    virtual std::shared_ptr<ColumnIndex> getColumnIndex() const;

    virtual std::shared_ptr<RowStream> getRowStream() const;
    
    virtual std::pair<Date, Date> getTimestampRange() const;

    virtual GenerateRowsWhereFunction
    generateRowsWhere(const SqlBindingScope & context,
                      const SqlExpression & where,
                      ssize_t offset,
                      ssize_t limit) const;

protected:
    // To initialize from a subclass
    CsvDataset(MldbServer * owner);

    struct Itl;
    std::shared_ptr<Itl> itl;
};


} // MLDB
} // Datacratic
