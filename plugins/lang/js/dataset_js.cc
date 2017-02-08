// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/** dataset_js.cc
    Jeremy Barnes, 14 June 2015
    Copyright (c) 2015 mldb.ai inc.  All rights reserved.

    JS interface for datasets.
*/

#include "dataset_js.h"
#include "mldb/core/dataset.h"


using namespace std;



namespace MLDB {


/*****************************************************************************/
/* DATASET JS                                                                */
/*****************************************************************************/

v8::Handle<v8::Object>
DatasetJS::
create(std::shared_ptr<Dataset> dataset, JsPluginContext * context)
{
    v8::Isolate* isolate = v8::Isolate::GetCurrent();
    auto obj = context->Dataset.Get(isolate)->GetFunction()->NewInstance();
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

    v8::Isolate* isolate = v8::Isolate::GetCurrent();
    EscapableHandleScope scope(isolate);

    auto fntmpl = CreateFunctionTemplate("Dataset");
    auto prototmpl = fntmpl->PrototypeTemplate();

    prototmpl->Set(String::NewFromUtf8(isolate, "recordRow"),
                   FunctionTemplate::New(isolate, recordRow));
    prototmpl->Set(String::NewFromUtf8(isolate, "recordRows"),
                   FunctionTemplate::New(isolate, recordRows));
    prototmpl->Set(String::NewFromUtf8(isolate, "recordColumn"),
                   FunctionTemplate::New(isolate, recordColumn));
    prototmpl->Set(String::NewFromUtf8(isolate, "recordColumns"),
                   FunctionTemplate::New(isolate, recordColumns));

    prototmpl->Set(String::NewFromUtf8(isolate, "commit"),
                   FunctionTemplate::New(isolate, commit));
    prototmpl->Set(String::NewFromUtf8(isolate, "status"),
                   FunctionTemplate::New(isolate, status));
    prototmpl->Set(String::NewFromUtf8(isolate, "id"),
                   FunctionTemplate::New(isolate, id));
    prototmpl->Set(String::NewFromUtf8(isolate, "type"),
                   FunctionTemplate::New(isolate, type));
    prototmpl->Set(String::NewFromUtf8(isolate, "config"),
                   FunctionTemplate::New(isolate, config));
        
    prototmpl->Set(String::NewFromUtf8(isolate, "getColumnPaths"),
                   FunctionTemplate::New(isolate, getColumnPaths));
    prototmpl->Set(String::NewFromUtf8(isolate, "getTimestampRange"),
                   FunctionTemplate::New(isolate, getTimestampRange));
        
    return scope.Escape(fntmpl);
}

void
DatasetJS::
recordRow(const v8::FunctionCallbackInfo<v8::Value> & args)
{
    JsContextScope scope(args.This());
    try {
        Dataset * dataset = getShared(args.This());
         
        auto rowName = JS::getArg<RowPath>(args, 0, "rowName");
        auto values = JS::getArg<std::vector<std::tuple<ColumnPath, CellValue, Date> > >(args, 1, "values", {});

        {
            //v8::Unlocker unlocker(args.GetIsolate());
            dataset->recordRow(std::move(rowName), std::move(values));
        }

        args.GetReturnValue().Set(args.This());
    } HANDLE_JS_EXCEPTIONS(args);
}

void
DatasetJS::
recordRows(const v8::FunctionCallbackInfo<v8::Value> & args)
{
    JsContextScope scope(args.This());
    try {
        Dataset * dataset = getShared(args.This());
        
        auto rows = JS::getArg<std::vector<std::pair<RowPath, std::vector<std::tuple<ColumnPath, CellValue, Date> > > > >(args, 0, "rows", {});

        {
            //v8::Unlocker unlocker(args.GetIsolate());
            dataset->recordRows(std::move(rows));
        }
        
        args.GetReturnValue().Set(args.This());
    } HANDLE_JS_EXCEPTIONS(args);
}

void
DatasetJS::
recordColumn(const v8::FunctionCallbackInfo<v8::Value> & args)
{
    JsContextScope scope(args.This());
    try {
        Dataset * dataset = getShared(args.This());
        
        auto columnName = JS::getArg<ColumnPath>(args, 0, "columnName");
        auto column = JS::getArg<std::vector<std::tuple<ColumnPath, CellValue, Date> > >(args, 1, "values", {});

        {
            //v8::Unlocker unlocker(args.GetIsolate());
            dataset->recordColumn(std::move(columnName), std::move(column));
        }

        args.GetReturnValue().Set(args.This());
    } HANDLE_JS_EXCEPTIONS(args);
}

void
DatasetJS::
recordColumns(const v8::FunctionCallbackInfo<v8::Value> & args)
{
    JsContextScope scope(args.This());
    try {
        Dataset * dataset = getShared(args.This());
            
        auto columns = JS::getArg<std::vector<std::pair<ColumnPath, std::vector<std::tuple<ColumnPath, CellValue, Date> > > > >(args, 0, "columns", {});
        dataset->recordColumns(std::move(columns));

        args.GetReturnValue().Set(args.This());
    } HANDLE_JS_EXCEPTIONS(args);
}

void
DatasetJS::
commit(const v8::FunctionCallbackInfo<v8::Value> & args)
{
    try {
        Dataset * dataset = getShared(args.This());
            
        dataset->commit();
            
        args.GetReturnValue().Set(args.This());
    } HANDLE_JS_EXCEPTIONS(args);
}

void
DatasetJS::
status(const v8::FunctionCallbackInfo<v8::Value> & args)
{
    try {
        Dataset * dataset = getShared(args.This());
            
        args.GetReturnValue().Set(JS::toJS(jsonEncode(dataset->getStatus())));
    } HANDLE_JS_EXCEPTIONS(args);
}
    
void
DatasetJS::
id(const v8::FunctionCallbackInfo<v8::Value> & args)
{
    try {
        Dataset * dataset = getShared(args.This());
            
        args.GetReturnValue().Set(JS::toJS(jsonEncode(dataset->getId())));
    } HANDLE_JS_EXCEPTIONS(args);
}
    
void
DatasetJS::
type(const v8::FunctionCallbackInfo<v8::Value> & args)
{
    try {
        Dataset * dataset = getShared(args.This());
            
        args.GetReturnValue().Set(JS::toJS(jsonEncode(dataset->getType())));
    } HANDLE_JS_EXCEPTIONS(args);
}
    
void
DatasetJS::
config(const v8::FunctionCallbackInfo<v8::Value> & args)
{
    try {
        Dataset * dataset = getShared(args.This());
        
        args.GetReturnValue().Set(JS::toJS(jsonEncode(dataset->getConfig())));
    } HANDLE_JS_EXCEPTIONS(args);
}

void
DatasetJS::
getColumnPaths(const v8::FunctionCallbackInfo<v8::Value> & args)
{
    try {
        Dataset * dataset = getShared(args.This());

        args.GetReturnValue().Set(JS::toJS(dataset->getColumnIndex()->getColumnPaths()));
    } HANDLE_JS_EXCEPTIONS(args);
}

void
DatasetJS::
getTimestampRange(const v8::FunctionCallbackInfo<v8::Value> & args)
{
    try {
        Dataset * dataset = getShared(args.This());
        args.GetReturnValue().Set(JS::toJS(dataset->getTimestampRange()));
    } HANDLE_JS_EXCEPTIONS(args);
}

} // namespace MLDB

