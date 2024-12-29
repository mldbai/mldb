/** python_core_components.cc
    Francois Maillet, 7 mars 2015
    Copyright (c) 2015 mldb.ai inc.  All rights reserved.

    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
*/


#include "python_entities.h"
#include "nanobind/nanobind.h"
#include <frameobject.h>

#include "python_converters.h"
#include "from_python_converter.h"
#include "callback.h"


using namespace std;



namespace MLDB {

/****************************************************************************/
/* DatasetPy                                                                */
/****************************************************************************/

DatasetPy::
DatasetPy(std::shared_ptr<Dataset> dataset)
    : dataset(dataset),
      context(std::make_shared<PythonContext>("DatasetPy", dataset->engine))
{
}

DatasetPy::
~DatasetPy()
{
    dataset.reset();
}

void DatasetPy::
recordRow(const RowPath & rowName, const std::vector<RowCellTuple> & columns) {
    auto nogil = releaseGil();
    dataset->recordRow(rowName, columns);
}

void DatasetPy::
recordRows(const std::vector<std::pair<RowPath, std::vector<RowCellTuple> > > & rows)
{
    auto nogil = releaseGil();
    dataset->recordRows(rows);
}
    
void  DatasetPy::
recordColumn(const ColumnPath & columnName,
             const std::vector<ColumnCellTuple> & columns)
{
    auto nogil = releaseGil();
    dataset->recordColumn(columnName, columns);
}

void  DatasetPy::
recordColumns(const std::vector<std::pair<ColumnPath, std::vector<ColumnCellTuple> > > & columns)
{
    auto nogil = releaseGil();
    dataset->recordColumns(columns);
}
    
void DatasetPy::
commit() {
    auto nogil = releaseGil();
    dataset->commit();
}

DatasetPy* DatasetPy::
createDataset(MldbPythonContext * mldbContext, const Json::Value & rawConfig)
{
    PolyConfig config = jsonDecode<PolyConfig>(rawConfig);

    // Possible recursive call; GIL must be released
    auto nogil = releaseGil();
    ExcAssert(mldbContext->getPyContext()->engine);
    return new DatasetPy(MLDB::createDataset
                         (mldbContext->getPyContext()->engine,
                          config));
}


/****************************************************************************/
/* PythonProcedure                                                           */
/****************************************************************************/

PythonProcedure::
PythonProcedure(MldbEngine * owner,
                PolyConfig config,
                const std::function<bool (const Json::Value &)> & onProgress)
    : Procedure(owner),
      context(std::make_shared<PythonContext>("ProcedurePy", owner))
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
#if 0
    auto localsPlugin = nanobind::object(nanobind::ptr(c));
    auto createProcedureEntity = 
        [=] (RestDirectory * peer,
             PolyConfig config,
             const std::function<bool (const Json::Value)> & onProgress)
        {
            PythonProcedure * procedure = new PythonProcedure(
                    PythonProcedure::getOwner(peer), config, onProgress);
            procedure->trainPy = [=] (const ProcedureRunConfig & training)
                {
                    try {
                        return nanobind::call<Json::Value>(
                            trainFunction, localsPlugin, jsonEncode(training).toString());

                    } catch (const nanobind::error_already_set & exc) {
                        ScriptException pyexc
                        = interpreter.convertException
                            (pyControl,
                             exc, "Procedure '"+name+"' train");

                        {
//                             std::unique_lock<std::mutex> guard(itl->logMutex);
//                             LOG(itl->loader) << jsonEncode(pyexc) << endl;
                        }

                        MLDB_TRACE_EXCEPTIONS(false);
                        throw AnnotatedException(400, "Exception creating procedure", 
                                                 pyexc);
                    }
                };
            return procedure;
        };

    Package package("python-TODO-package-name");

    registerProcedureType(package, name, description,
                          createProcedureEntity, nullptr,
                          nullptr, nullptr, {} /* flags */);
#endif
}

/****************************************************************************/
/* PythonFunction                                                              */
/****************************************************************************/

PythonFunction::
PythonFunction(MldbEngine * owner,
               PolyConfig config,
               const std::function<bool (const Json::Value &)> & onProgress)
    : Function(owner, config),
      context(std::make_shared<PythonContext>("FunctionPy", owner))
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

namespace {

void pythonEntitiesInit(const EnterThreadToken & thread)
{
#if 0
    from_python_converter< RowCellTuple,
                           Tuple3ElemConverter<ColumnPath, CellValue, Date> >();

    from_python_converter< std::vector<RowCellTuple>,
                           VectorConverter<RowCellTuple>>();

    from_python_converter< std::pair<RowPath, std::vector<RowCellTuple> >,
                           PairConverter<RowPath, std::vector<RowCellTuple> > >();

    from_python_converter< std::vector<std::pair<RowPath, std::vector<RowCellTuple> > >,
                           VectorConverter<std::pair<RowPath, std::vector<RowCellTuple> > > >();

    from_python_converter< ColumnCellTuple,
                           Tuple3ElemConverter<RowPath, CellValue, Date> >();

    from_python_converter< std::vector<ColumnCellTuple>,
                           VectorConverter<ColumnCellTuple>>();

    from_python_converter< std::pair<ColumnPath, std::vector<ColumnCellTuple> >,
                           PairConverter<ColumnPath, std::vector<ColumnCellTuple> > >();

    from_python_converter< std::vector<std::pair<ColumnPath, std::vector<ColumnCellTuple> > >,
                           VectorConverter<std::pair<ColumnPath, std::vector<ColumnCellTuple> > > >();

    nanobind::class_<DatasetPy>("dataset", nanobind::no_init)
        .def("record_row", &DatasetPy::recordRow)
        .def("record_rows", &DatasetPy::recordRows)
        .def("record_column", &DatasetPy::recordColumn)
        .def("record_columns", &DatasetPy::recordColumns)
        .def("commit", &DatasetPy::commit);

    nanobind::class_<FunctionInfo, boost::noncopyable>("function_info", nanobind::no_init)
        ;
#endif
}

// Arrange for the above function to be run at the appropriate moment
// when there is a proper python environment set up.  There is no
// proper environment on shared initialization, so it can't be run
// from AtInit.

RegisterPythonInitializer regMe(&pythonEntitiesInit);


} // file scope

} // namespace MLDB

