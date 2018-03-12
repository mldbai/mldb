/* import_parquet.h                                            -*- C++ -*-
   Jeremy Barnes, May 20, 2017
   Copyright (c) 2017 mldb.ai inc.  All rights reserved.
   
   This file is part of MLDB. Copyright 2017 mldb.ai inc. All rights reserved.

   Procedure that imports Apache Parquet files.
*/


#pragma once

#include "mldb/core/dataset.h"
#include "mldb/core/procedure.h"

namespace MLDB {


struct ImportParquetConfig : public ProcedureConfig  {

    static constexpr const char * name = "import.parquet";

    Url dataFileUrl;
    PolyConfigT<Dataset> outputDataset = DefaultType("tabular");
    int64_t limit = -1;
    int64_t offset = 0;

    SelectExpression select = SelectExpression::STAR;
    std::shared_ptr<SqlExpression> where = SqlExpression::TRUE;
    std::shared_ptr<SqlExpression> named = SqlExpression::parse("rowNumber()");
    std::shared_ptr<SqlExpression> timestamp = SqlExpression::parse("fileTimestamp()");
};

DECLARE_STRUCTURE_DESCRIPTION(ImportParquetConfig);


/*****************************************************************************/
/* IMPORT PARQUET PROCEDURE                                                  */
/*****************************************************************************/

struct ImportParquetProcedure: public Procedure {

    ImportParquetProcedure(MldbServer * owner,
                        PolyConfig config,
                        const std::function<bool (const Json::Value &)> & onProgress);

    virtual RunOutput run(const ProcedureRunConfig & run,
                          const std::function<bool (const Json::Value &)> & onProgress) const;
    
    virtual Any getStatus() const;
    
    ImportParquetConfig config;
};

} // namespace MLDB

