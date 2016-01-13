// This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

/** text_line_dataset.h                                           -*- C++ -*-
    Jeremy Barnes, 16 December 2014
    Copyright (c) 2014 Datacratic Inc.  All rights reserved.

*/

#include "mldb/core/dataset.h"

#pragma once

namespace Datacratic {
namespace MLDB {


/*****************************************************************************/
/* TEXT LINE DATASET                                                         */
/*****************************************************************************/

/** Dataset type that iterates a text file line by line.
*/

struct TextLineDataset: public Dataset {

    TextLineDataset(MldbServer * owner,
                    PolyConfig config,
                    const std::function<bool (const Json::Value &)> & onProgress);
    
    virtual ~TextLineDataset();
    
    virtual Any getStatus() const;

    virtual std::shared_ptr<MatrixView> getMatrixView() const;

    virtual std::shared_ptr<ColumnIndex> getColumnIndex() const;

    virtual std::shared_ptr<RowStream> getRowStream() const;

    virtual std::pair<Date, Date> getTimestampRange() const;

protected:
    // To initialize from a subclass
    TextLineDataset(MldbServer * owner);

    struct Itl;
    std::shared_ptr<Itl> itl;
};


} // MLDB
} // Datacratic
