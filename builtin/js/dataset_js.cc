// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/** dataset_js.cc
    Jeremy Barnes, 14 June 2015
    Copyright (c) 2015 mldb.ai inc.  All rights reserved.

    JS interface for datasets.
*/

#include "dataset_js.h"
#include "mldb/core/dataset.h"
#include "js_common.h"

using namespace std;



namespace MLDB {


/*****************************************************************************/
/* DATASET JS                                                                */
/*****************************************************************************/

v8::Handle<v8::Object>
DatasetJS::
create(std::shared_ptr<Dataset> dataset, JsPluginContext * pluginContext)
{
    return doCreateWrapper<DatasetJS>(std::move(dataset), pluginContext, pluginContext->Dataset);
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

#define ADD_METHOD(name) JS::addMethod(isolate, prototmpl, #name, FunctionTemplate::New(isolate, name))
    ADD_METHOD(recordRow);
    ADD_METHOD(recordRows);
    ADD_METHOD(recordColumn);
    ADD_METHOD(recordColumns);
    ADD_METHOD(commit);
    ADD_METHOD(status);
    ADD_METHOD(id);
    ADD_METHOD(type);
    ADD_METHOD(config);
    ADD_METHOD(getColumnPaths);
    ADD_METHOD(getTimestampRange);
#undef ADD_METHOD 

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

        JS::setReturnValue(args, args.This());
    } HANDLE_JS_EXCEPTIONS(args);
}

void
DatasetJS::
recordRows(const v8::FunctionCallbackInfo<v8::Value> & args)
{
    auto isolate = v8::Isolate::GetCurrent();
    auto context = isolate->GetCurrentContext();

    JsContextScope scope(args.This());
    try {
        Dataset * dataset = getShared(args.This());
        
        // Look at rows
        // If it's an array of arrays, it's in the vector<pair> format
        // If it's an array of objects, it's in vector<MatrixNamedRow> format

        auto array = args[0].As<v8::Array>();
        if (array.IsEmpty())
            throw AnnotatedException(400, "value " + JS::cstr(args[0]) + " is not an array");
        if (array->Length() == 0)
            return;

        auto el = JS::get(context, array, 0);
        if (el->IsArray()) {
            // Note: goes first, because an array is also an object
            auto rows = JS::getArg<std::vector<std::pair<RowPath, std::vector<std::tuple<ColumnPath, CellValue, Date> > > > >(args, 0, "rows", {});
            v8::Unlocker unlocker(args.GetIsolate());
            dataset->recordRows(std::move(rows));
        }
        else if (el->IsObject()) {
            std::vector<std::pair<RowPath, ExpressionValue> > toRecord;
            toRecord.reserve(array->Length());

            auto columns = JS::createString(args.GetIsolate(), "columns");
            auto rowPath = JS::createString(args.GetIsolate(), "rowPath");

            for (size_t i = 0;  i < array->Length();  ++i) {
                MatrixNamedRow row;
                auto obj = JS::getObject(context, array, i);

                if (obj.IsEmpty()) {
                    throw AnnotatedException(400, "recordRow element is not object");
                }

                toRecord.emplace_back(from_js(JS::get(context, obj, rowPath), &row.rowName),
                                      from_js(JS::get(context, obj, columns), &row.columns));
            }
            
            v8::Unlocker unlocker(args.GetIsolate());
            dataset->recordRowsExpr(std::move(toRecord));
        }
        else throw AnnotatedException(400, "Can't call recordRows with argument "
                                       + JS::cstr(el));
        
        JS::setReturnValue(args, args.This());
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

        JS::setReturnValue(args, args.This());
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

        JS::setReturnValue(args, args.This());
    } HANDLE_JS_EXCEPTIONS(args);
}

void
DatasetJS::
commit(const v8::FunctionCallbackInfo<v8::Value> & args)
{
    try {
        Dataset * dataset = getShared(args.This());
            
        dataset->commit();
            
        JS::setReturnValue(args, args.This());
    } HANDLE_JS_EXCEPTIONS(args);
}

void
DatasetJS::
status(const v8::FunctionCallbackInfo<v8::Value> & args)
{
    try {
        Dataset * dataset = getShared(args.This());
            
        JS::setReturnValue(args, JS::toJS(jsonEncode(dataset->getStatus())));
    } HANDLE_JS_EXCEPTIONS(args);
}
    
void
DatasetJS::
id(const v8::FunctionCallbackInfo<v8::Value> & args)
{
    try {
        Dataset * dataset = getShared(args.This());
            
        JS::setReturnValue(args, JS::toJS(jsonEncode(dataset->getId())));
    } HANDLE_JS_EXCEPTIONS(args);
}
    
void
DatasetJS::
type(const v8::FunctionCallbackInfo<v8::Value> & args)
{
    try {
        Dataset * dataset = getShared(args.This());
            
        JS::setReturnValue(args, JS::toJS(jsonEncode(dataset->getType())));
    } HANDLE_JS_EXCEPTIONS(args);
}
    
void
DatasetJS::
config(const v8::FunctionCallbackInfo<v8::Value> & args)
{
    try {
        Dataset * dataset = getShared(args.This());
        
        JS::setReturnValue(args, JS::toJS(jsonEncode(dataset->getConfig())));
    } HANDLE_JS_EXCEPTIONS(args);
}

void
DatasetJS::
getColumnPaths(const v8::FunctionCallbackInfo<v8::Value> & args)
{
    try {
        Dataset * dataset = getShared(args.This());

        JS::setReturnValue(args, JS::toJS(dataset->getColumnIndex()->getColumnPaths()));
    } HANDLE_JS_EXCEPTIONS(args);
}

void
DatasetJS::
getTimestampRange(const v8::FunctionCallbackInfo<v8::Value> & args)
{
    try {
        Dataset * dataset = getShared(args.This());
        JS::setReturnValue(args, JS::toJS(dataset->getTimestampRange()));
    } HANDLE_JS_EXCEPTIONS(args);
}

} // namespace MLDB

