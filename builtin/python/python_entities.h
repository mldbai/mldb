/** python_core_components.h                                       -*- C++ -*-
    Francois Maillet, 6 mars 2015
    Copyright (c) 2015 mldb.ai inc.  All rights reserved.

    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
    
    Wrappers for core MLDB objects
*/

#pragma once

#include "python_plugin_context.h"

#include "mldb/core/dataset.h"
#include "mldb/core/function.h"
#include "mldb/core/procedure.h"


using namespace std;
using namespace MLDB::Python;


namespace MLDB {


typedef std::tuple<ColumnPath, CellValue, Date> RowCellTuple;
typedef std::tuple<ColumnPath, CellValue, Date> ColumnCellTuple;

/****************************************************************************/
/* DatasetPy                                                                */
/****************************************************************************/

struct DatasetPy {

    DatasetPy(std::shared_ptr<Dataset> dataset) :
        dataset(dataset),
        interpreter(dataset->engine)
    {
    }

    ~DatasetPy();
    
    void recordRow(const RowPath & rowName,
                   const std::vector<RowCellTuple> & columns);
    void recordRows(const std::vector<std::pair<RowPath, std::vector<RowCellTuple> > > & rows);
    
    void recordColumn(const ColumnPath & columnName,
                      const std::vector<ColumnCellTuple> & rows);
    void recordColumns(const std::vector<std::pair<ColumnPath, std::vector<ColumnCellTuple> > > & columns);

    void commit();
    
    std::shared_ptr<Dataset> dataset;

    MldbPythonInterpreter interpreter;

    static DatasetPy* createDataset(MldbPythonContext * c,
                                    const Json::Value & config);

};


/****************************************************************************/
/* PythonProcedure                                                           */
/****************************************************************************/

struct PythonProcedure: public Procedure {

    PythonProcedure(MldbEngine * owner,
                PolyConfig config,
                const std::function<bool (const Json::Value &)> & onProgress);

    virtual RunOutput run(const ProcedureRunConfig & run,
                          const std::function<bool (const Json::Value &)> & onProgress) const;

    virtual Any getStatus() const;

    Json::Value procedureConfig;
    
    std::function<Json::Value (const ProcedureRunConfig & training)> trainPy;

    MldbPythonInterpreter interpreter;
    
    static void createPythonProcedure(MldbPythonContext * c,
                                     const std::string & name, 
                                     const std::string & description,
                                     PyObject * trainFunction);
};



/****************************************************************************/
/* PythonFunction                                                           */
/****************************************************************************/

struct PythonFunction: public Function {

    PythonFunction(MldbEngine * owner,
                PolyConfig config,
                const std::function<bool (const Json::Value &)> & onProgress);

    virtual Any getStatus() const;

    Json::Value functionConfig;
    
    MldbPythonInterpreter interpreter;

    static void createPythonFunction(PythonPluginContext * c,
                                  const std::string & name);
};





} // namespace MLDB

