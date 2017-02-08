/* importextprocedure.h                                            -*- C++ -*-
    Mathieu Marquis Bolduc, February 12, 2016
    Copyright (c) 2016 mldb.ai inc.  All rights reserved.

    This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.

    Procedure that reads text files into an indexed dataset.
*/


#pragma once

#include "mldb/core/dataset.h"
#include "mldb/core/procedure.h"
#include "mldb/core/function.h"
#include "mldb/ml/value_descriptions.h"
#include "mldb/types/optional.h"


namespace MLDB {


struct ImportTextConfig : public ProcedureConfig  {
    static constexpr const char * name = "import.text";

    ImportTextConfig()
        : delimiter(","),
          quoter("\""),
          encoding("utf-8"),
          replaceInvalidCharactersWith(""),
          limit(-1),
          offset(0),
          ignoreBadLines(false),
          structuredColumnNames(false),
          allowMultiLines(false),
          autoGenerateHeaders(false),
          select(SelectExpression::STAR),
          where(SqlExpression::TRUE),
          named(SqlExpression::parse("lineNumber()")),
          timestamp(SqlExpression::parse("fileTimestamp()"))
    {
        outputDataset.withType("tabular");
    }

    Url dataFileUrl;
    PolyConfigT<Dataset> outputDataset;
    std::vector<Utf8String> headers;
    std::string delimiter;
    std::string quoter;
    std::string encoding;
    Utf8String replaceInvalidCharactersWith;
    int64_t limit;
    int64_t offset;
    bool ignoreBadLines;
    bool structuredColumnNames;
    bool allowMultiLines;
    bool autoGenerateHeaders;

    SelectExpression select;               ///< What to select from the CSV
    std::shared_ptr<SqlExpression> where;  ///< Filter for the CSV
    std::shared_ptr<SqlExpression> named;  ///< Row name to output
    std::shared_ptr<SqlExpression> timestamp;   ///< Timestamp for row

    PolyConfigT<Dataset> output;

};

DECLARE_STRUCTURE_DESCRIPTION(ImportTextConfig);


/*****************************************************************************/
/* IMPORT TEXT PROCEDURE                                                     */
/*****************************************************************************/

struct ImportTextProcedure: public Procedure {

    ImportTextProcedure(MldbServer * owner,
                PolyConfig config,
                const std::function<bool (const Json::Value &)> & onProgress);

    virtual RunOutput run(const ProcedureRunConfig & run,
                          const std::function<bool (const Json::Value &)> & onProgress) const;

    virtual Any getStatus() const;

    ImportTextConfig config;

private:

};

} // namespace MLDB

