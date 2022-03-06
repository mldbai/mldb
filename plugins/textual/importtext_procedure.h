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
#include "mldb/types/optional.h"
#include "mldb/types/regex.h"
#include "mldb/block/content_descriptor.h"
#include "dataset_builder.h"

namespace MLDB {


/*****************************************************************************/
/* IMPORT TEXT CONFIG                                                        */
/*****************************************************************************/

struct ImportTextConfig : public ProcedureConfig, public DatasetBuilderConfig  {
    static constexpr const char * name = "import.text";

    ContentDescriptor dataFileUrl;
    std::vector<Utf8String> headers;
    std::string delimiter = ",";
    std::string quoter = "\"";
    std::string encoding = "utf-8";
    Utf8String replaceInvalidCharactersWith;
    int64_t limit = -1;
    int64_t offset = 0;
    int64_t preHeaderOffset = 0;
    bool ignoreBadLines = false;
    bool structuredColumnNames = false;
    bool allowMultiLines = false;
    bool autoGenerateHeaders = false;
    bool ignoreExtraColumns = false;
    bool processExcelFormulas = true;
    Regex skipLineRegex;
};

DECLARE_STRUCTURE_DESCRIPTION(ImportTextConfig);


/*****************************************************************************/
/* IMPORT TEXT PROCEDURE                                                     */
/*****************************************************************************/

struct ImportTextProcedure: public Procedure {

    ImportTextProcedure(MldbEngine * owner,
                PolyConfig config,
                const std::function<bool (const Json::Value &)> & onProgress);

    virtual RunOutput run(const ProcedureRunConfig & run,
                          const std::function<bool (const Json::Value &)> & onProgress) const;

    virtual Any getStatus() const;

    ImportTextConfig config;

private:

};

} // namespace MLDB

