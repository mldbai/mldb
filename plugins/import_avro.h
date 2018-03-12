/* import_avro.h                                            -*- C++ -*-
   Jeremy Barnes, May 20, 2017
   Copyright (c) 2017 mldb.ai inc.  All rights reserved.
   
   This file is part of MLDB. Copyright 2017 mldb.ai inc. All rights reserved.

   Procedure that imports Apache Avro files.
*/


#pragma once

#include "mldb/core/dataset.h"
#include "mldb/core/procedure.h"

namespace MLDB {


struct ImportAvroConfig : public ProcedureConfig  {

    static constexpr const char * name = "import.avro";

    Url dataFileUrl;
    PolyConfigT<Dataset> outputDataset = DefaultType("tabular");
    int64_t limit = -1;
    int64_t offset = 0;

    SelectExpression select = SelectExpression::STAR;
    std::shared_ptr<SqlExpression> where = SqlExpression::TRUE;
    std::shared_ptr<SqlExpression> named = SqlExpression::parse("lineNumber()");
    std::shared_ptr<SqlExpression> timestamp = SqlExpression::parse("fileTimestamp()");
};

DECLARE_STRUCTURE_DESCRIPTION(ImportAvroConfig);


/*****************************************************************************/
/* IMPORT AVRO PROCEDURE                                                     */
/*****************************************************************************/

struct ImportAvroProcedure: public Procedure {

    ImportAvroProcedure(MldbServer * owner,
                        PolyConfig config,
                        const std::function<bool (const Json::Value &)> & onProgress);

    virtual RunOutput run(const ProcedureRunConfig & run,
                          const std::function<bool (const Json::Value &)> & onProgress) const;
    
    virtual Any getStatus() const;
    
    ImportAvroConfig config;
};

} // namespace MLDB

