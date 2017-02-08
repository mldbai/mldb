/** python_core_components.cc
    Francois Maillet, 7 mars 2015
    Copyright (c) 2015 mldb.ai inc.  All rights reserved.

    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
*/


#include "python_entities.h"
#include <boost/python.hpp>
#include <boost/python/return_value_policy.hpp>
#include <frameobject.h>

#include "python_converters.h"
#include "from_python_converter.h"
#include "callback.h"
#include <boost/python/to_python_converter.hpp>


using namespace std;



namespace MLDB {

/****************************************************************************/
/* DatasetPy                                                                */
/****************************************************************************/

void DatasetPy::
recordRow(const RowPath & rowName, const std::vector<RowCellTuple> & columns) {
    dataset->recordRow(rowName, columns);
}

void DatasetPy::
recordRows(const std::vector<std::pair<RowPath, std::vector<RowCellTuple> > > & rows)
{
    dataset->recordRows(rows);
}
    
void  DatasetPy::
recordColumn(const ColumnPath & columnName,
             const std::vector<ColumnCellTuple> & columns)
{
    dataset->recordColumn(columnName, columns);
}

void  DatasetPy::
recordColumns(const std::vector<std::pair<ColumnPath, std::vector<ColumnCellTuple> > > & columns)
{
    dataset->recordColumns(columns);
}
    
void DatasetPy::
commit() {
    dataset->commit();
}

DatasetPy* DatasetPy::
createDataset(MldbPythonContext * mldbContext, const Json::Value & rawConfig)
{
    PolyConfig config = jsonDecode<PolyConfig>(rawConfig);

    auto dataset = MLDB::createDataset(mldbContext->getPyContext()->server,
                                       config);
    return new DatasetPy(dataset);
}


/****************************************************************************/
/* PythonProcedure                                                           */
/****************************************************************************/

PythonProcedure::
PythonProcedure(MldbServer * owner,
               PolyConfig config,
               const std::function<bool (const Json::Value &)> & onProgress)
    : Procedure(owner)
{
    procedureConfig = config.params.asJson();
}

RunOutput PythonProcedure::
run(const ProcedureRunConfig & run,
      const std::function<bool (const Json::Value &)> & onProgress) const
{
    return RunOutput(trainPy(run));
}

Any PythonProcedure::
getStatus() const
{
    return Any();
}

void PythonProcedure::
createPythonProcedure(MldbPythonContext * c,
                     const std::string & name,
                     const std::string & description,
                     PyObject * trainFunction)
{
    auto localsPlugin = boost::python::object(boost::python::ptr(c));
    auto createProcedureEntity = 
        [=] (RestDirectory * peer,
             PolyConfig config,
             const std::function<bool (const Json::Value)> & onProgress)
        {
            PythonProcedure * procedure = new PythonProcedure(
                    PythonProcedure::getOwner(peer), config, onProgress);
            procedure->trainPy = [=] (const ProcedureRunConfig & training)
                {
                    PythonSubinterpreter pyControl;

                    cout << "calling python function" << endl;
                    try {
                        return boost::python::call<Json::Value>(
                            trainFunction, localsPlugin, jsonEncode(training).toString());

                    } catch (const boost::python::error_already_set & exc) {
                        ScriptException pyexc = convertException(pyControl,
                                exc, "Procedure '"+name+"' train");

                        {
//                             std::unique_lock<std::mutex> guard(itl->logMutex);
//                             LOG(itl->loader) << jsonEncode(pyexc) << endl;
                        }

                        MLDB_TRACE_EXCEPTIONS(false);
                        throw HttpReturnException(400, "Exception creating procedure", 
                                                  pyexc);
                    }
                };
            return procedure;
        };

    Package package("python-TODO-package-name");

    registerProcedureType(package, name, description,
                          createProcedureEntity, nullptr,
                          nullptr, nullptr, {} /* flags */);
}


/****************************************************************************/
/* PythonFunction                                                              */
/****************************************************************************/

PythonFunction::
PythonFunction(MldbServer * owner,
            PolyConfig config,
            const std::function<bool (const Json::Value &)> & onProgress)
    : Function(owner, config)
{
    functionConfig = config.params.asJson();
}

Any PythonFunction::
getStatus() const
{
    return Any();
}



void PythonFunction::
createPythonFunction(PythonPluginContext * c,
                  const std::string & name)
{

}

} // namespace MLDB

