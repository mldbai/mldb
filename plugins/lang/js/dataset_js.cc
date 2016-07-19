// This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

/** dataset_js.cc
    Jeremy Barnes, 14 June 2015
    Copyright (c) 2015 Datacratic Inc.  All rights reserved.

    JS interface for datasets.
*/

#include "dataset_js.h"
#include "mldb/core/dataset.h"
#include "id_js.h"


using namespace std;


namespace Datacratic {
namespace MLDB {


/*****************************************************************************/
/* DATASET JS                                                                */
/*****************************************************************************/

v8::Handle<v8::Object>
DatasetJS::
create(std::shared_ptr<Dataset> dataset, JsThreadContext * context)
{
    auto obj = context->Dataset->GetFunction()->NewInstance();
    auto * wrapped = new DatasetJS();
    wrapped->dataset = dataset;
    wrapped->wrap(obj, context);
    return obj;
}

Dataset *
DatasetJS::
getShared(const v8::Handle<v8::Object> & val)
{
    return reinterpret_cast<DatasetJS *>
        (v8::Handle<v8::External>::Cast
         (val->GetInternalField(0))->Value())->dataset.get();
}

v8::Local<v8::FunctionTemplate>
DatasetJS::
registerMe()
{
    using namespace v8;

    HandleScope scope;

    auto fntmpl = CreateFunctionTemplate("Dataset");
    auto prototmpl = fntmpl->PrototypeTemplate();

    prototmpl->Set(String::New("recordRow"), FunctionTemplate::New(recordRow));
    prototmpl->Set(String::New("recordRows"), FunctionTemplate::New(recordRows));
    prototmpl->Set(String::New("recordColumn"), FunctionTemplate::New(recordColumn));
    prototmpl->Set(String::New("recordColumns"), FunctionTemplate::New(recordColumns));

    prototmpl->Set(String::New("commit"), FunctionTemplate::New(commit));
    prototmpl->Set(String::New("status"), FunctionTemplate::New(status));
    prototmpl->Set(String::New("id"), FunctionTemplate::New(id));
    prototmpl->Set(String::New("type"), FunctionTemplate::New(type));
    prototmpl->Set(String::New("config"), FunctionTemplate::New(config));
        
    prototmpl->Set(String::New("getColumnNames"), FunctionTemplate::New(getColumnNames));
    prototmpl->Set(String::New("getTimestampRange"), FunctionTemplate::New(getTimestampRange));
        
    return scope.Close(fntmpl);
}

v8::Handle<v8::Value>
DatasetJS::
recordRow(const v8::Arguments & args)
{
    JsContextScope scope(args.This());
    try {
        Dataset * dataset = getShared(args.This());
            
        dataset->recordRow(JS::getArg<RowName>(args, 0, "rowName"),
                           JS::getArg<std::vector<std::tuple<ColumnName, CellValue, Date> > >(args, 1, "values", {}));

        return args.This();
    } HANDLE_JS_EXCEPTIONS;
}

v8::Handle<v8::Value>
DatasetJS::
recordRows(const v8::Arguments & args)
{
    JsContextScope scope(args.This());
    try {
        Dataset * dataset = getShared(args.This());
            
        dataset->recordRows(JS::getArg<std::vector<std::pair<RowName, std::vector<std::tuple<ColumnName, CellValue, Date> > > > >(args, 0, "rows", {}));

        return args.This();
    } HANDLE_JS_EXCEPTIONS;
}

v8::Handle<v8::Value>
DatasetJS::
recordColumn(const v8::Arguments & args)
{
    JsContextScope scope(args.This());
    try {
        Dataset * dataset = getShared(args.This());
            
        dataset->recordColumn(JS::getArg<ColumnName>(args, 0, "columnName"),
                              JS::getArg<std::vector<std::tuple<ColumnName, CellValue, Date> > >(args, 1, "values", {}));

        return args.This();
    } HANDLE_JS_EXCEPTIONS;
}

v8::Handle<v8::Value>
DatasetJS::
recordColumns(const v8::Arguments & args)
{
    JsContextScope scope(args.This());
    try {
        Dataset * dataset = getShared(args.This());
            
        dataset->recordColumns(JS::getArg<std::vector<std::pair<ColumnName, std::vector<std::tuple<ColumnName, CellValue, Date> > > > >(args, 0, "columns", {}));

        return args.This();
    } HANDLE_JS_EXCEPTIONS;
}

v8::Handle<v8::Value>
DatasetJS::
commit(const v8::Arguments & args)
{
    try {
        Dataset * dataset = getShared(args.This());
            
        dataset->commit();
            
        return args.This();
    } HANDLE_JS_EXCEPTIONS;
}

v8::Handle<v8::Value>
DatasetJS::
status(const v8::Arguments & args)
{
    try {
        Dataset * dataset = getShared(args.This());
            
        return JS::toJS(jsonEncode(dataset->getStatus()));
    } HANDLE_JS_EXCEPTIONS;
}
    
v8::Handle<v8::Value>
DatasetJS::
id(const v8::Arguments & args)
{
    try {
        Dataset * dataset = getShared(args.This());
            
        return JS::toJS(dataset->getId());
    } HANDLE_JS_EXCEPTIONS;
}
    
v8::Handle<v8::Value>
DatasetJS::
type(const v8::Arguments & args)
{
    try {
        Dataset * dataset = getShared(args.This());
            
        return JS::toJS(dataset->getType());
    } HANDLE_JS_EXCEPTIONS;
}
    
v8::Handle<v8::Value>
DatasetJS::
config(const v8::Arguments & args)
{
    try {
        Dataset * dataset = getShared(args.This());
        
        return JS::toJS(jsonEncode(dataset->getConfig()));
    } HANDLE_JS_EXCEPTIONS;
}
    
v8::Handle<v8::Value>
DatasetJS::
getColumnNames(const v8::Arguments & args)
{
    try {
        Dataset * dataset = getShared(args.This());

        return JS::toJS(dataset->getColumnIndex()->getColumnNames());
    } HANDLE_JS_EXCEPTIONS;
}

v8::Handle<v8::Value>
DatasetJS::
getTimestampRange(const v8::Arguments & args)
{
    try {
        Dataset * dataset = getShared(args.This());
        return JS::toJS(dataset->getTimestampRange());
    } HANDLE_JS_EXCEPTIONS;
}

} // namespace MLDB
} // namespace Datacratic
