/** mldb_js.cc
    Jeremy Barnes, 20 June 2015
    Copyright (c) 2015 mldb.ai inc.  All rights reserved.

    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

    Javascript bindings for MLDB.
*/

#include "mldb_js.h"
#include "mldb/types/basic_value_descriptions.h"
#include "mldb/rest/in_process_rest_connection.h"
#include "mldb/core/dataset.h"
#include "mldb/core/procedure.h"
#include "mldb/core/function.h"
#include "mldb/core/sensor.h"
#include "mldb/base/optimized_path.h"
#include "mldb/core/dataset_scope.h"
#include "mldb/core/analytics.h"
#include "procedure_js.h"
#include "function_js.h"
#include "sensor_js.h"
#include "dataset_js.h"
#include "mldb/core/mldb_engine.h"
#include "mldb/sql/sql_utils.h"
#include <random>

#include "mldb/vfs/fs_utils.h"
#include "mldb/vfs/filter_streams.h"
#include "mldb/utils/json_diff.h"
#include "mldb/types/map_description.h"


using namespace std;



namespace MLDB {


/*****************************************************************************/
/* CELL VALUE JS                                                             */
/*****************************************************************************/

struct CellValueJS::Methods {
};

v8::Handle<v8::Object>
CellValueJS::
create(CellValue value, JsPluginContext * pluginContext)
{
    return doCreateWrapper<CellValueJS>(std::move(value), pluginContext, pluginContext->CellValue);
}

CellValue &
CellValueJS::
getShared(const v8::Handle<v8::Object> & val)
{
    return reinterpret_cast<CellValueJS *>
        (v8::Handle<v8::External>::Cast
         (val->GetInternalField(0))->Value())->val;
}

v8::Local<v8::FunctionTemplate>
CellValueJS::
registerMe()
{
    using namespace v8;

    v8::Isolate* isolate = v8::Isolate::GetCurrent();
    EscapableHandleScope scope(isolate);

    auto fntmpl = CreateFunctionTemplate("Atom");

    return scope.Escape(fntmpl);
}


/*****************************************************************************/
/* STREAM JS                                                                 */
/*****************************************************************************/

/** Interface around an istream. */

struct StreamJS::Methods {
    static void
    readLine(const v8::FunctionCallbackInfo<v8::Value> & args)
    {
        try {
            auto stream = getShared(args.This());

            string nextLine;
            if(!getline(*stream, nextLine))
                throw MLDB::Exception("End of file");

            while (!nextLine.empty() && nextLine[nextLine.size() - 1] == '\r')
                nextLine.erase(nextLine.size() - 1);

            args.GetReturnValue().Set(JS::toJS(nextLine));
        } HANDLE_JS_EXCEPTIONS(args);
    }

    static void
    readJson(const v8::FunctionCallbackInfo<v8::Value> & args)
    {
        try {
            auto stream = getShared(args.This());

            Json::Value val = Json::parse(*stream);

            args.GetReturnValue().Set(JS::toJS(val));
        } HANDLE_JS_EXCEPTIONS(args);
    }

    static void
    readU8(const v8::FunctionCallbackInfo<v8::Value> & args)
    {
        try {
            auto stream = getShared(args.This());

            union {
                char byte;
                uint8_t u;
            };

            stream->read(&byte, 1);

            if (stream->gcount() < 1)
                throw MLDB::Exception("End of file");
            
            args.GetReturnValue().Set(JS::toJS(u));
        } HANDLE_JS_EXCEPTIONS(args);
    }

    static void
    readI8(const v8::FunctionCallbackInfo<v8::Value> & args)
    {
        try {
            auto stream = getShared(args.This());
            union {
                char byte;
                int8_t i;
            };
            stream->read(&byte, 1);

            if (stream->gcount() < 1)
                throw MLDB::Exception("End of file");
            
            args.GetReturnValue().Set(JS::toJS(i));
        } HANDLE_JS_EXCEPTIONS(args);
    }

    static void
    readU32LE(const v8::FunctionCallbackInfo<v8::Value> & args)
    {
        try {
            auto stream = getShared(args.This());
            union {
                char bytes[4];
                uint32_t u;
            };
            stream->read(bytes, 4);

            if (stream->gcount() < 4)
                throw MLDB::Exception("End of file");
            
            args.GetReturnValue().Set(JS::toJS(u));
        } HANDLE_JS_EXCEPTIONS(args);
    }

    static void
    readU32BE(const v8::FunctionCallbackInfo<v8::Value> & args)
    {
        try {
            auto stream = getShared(args.This());
            union {
                char bytes[4];
                unsigned char ubytes[4];
            };
            stream->read(bytes, 4);

            if (stream->gcount() < 4)
                throw MLDB::Exception("End of file");
            
            uint32_t u
                = ubytes[3]
                | ubytes[2] << 8
                | ubytes[1] << 16
                | ubytes[0] << 24;

            args.GetReturnValue().Set(JS::toJS(u));
        } HANDLE_JS_EXCEPTIONS(args);
    }

    static void
    readI32LE(const v8::FunctionCallbackInfo<v8::Value> & args)
    {
        try {
            auto stream = getShared(args.This());
            union {
                char bytes[4];
                unsigned char ubytes[4];
                int32_t u;
            };
            stream->read(bytes, 4);

            if (stream->gcount() < 4)
                throw MLDB::Exception("End of file");
            
            args.GetReturnValue().Set(JS::toJS(u));
        } HANDLE_JS_EXCEPTIONS(args);
    }

    static void
    readI32BE(const v8::FunctionCallbackInfo<v8::Value> & args)
    {
        try {
            auto stream = getShared(args.This());
            union {
                char bytes[4];
                unsigned char ubytes[4];
            };
            stream->read(bytes, 4);

            if (stream->gcount() < 4)
                throw MLDB::Exception("End of file");
            
            int32_t u
                = ubytes[3]
                | ubytes[2] << 8
                | ubytes[1] << 16
                | ubytes[0] << 24;

            args.GetReturnValue().Set(JS::toJS(u));
        } HANDLE_JS_EXCEPTIONS(args);
    }

    static void
    readU16LE(const v8::FunctionCallbackInfo<v8::Value> & args)
    {
        try {
            auto stream = getShared(args.This());
            union {
                char bytes[2];
                uint16_t u;
            };
            stream->read(bytes, 2);

            if (stream->gcount() < 2)
                throw MLDB::Exception("End of file");
            
            args.GetReturnValue().Set(JS::toJS(u));
        } HANDLE_JS_EXCEPTIONS(args);
    }

    static void
    readU16BE(const v8::FunctionCallbackInfo<v8::Value> & args)
    {
        try {
            auto stream = getShared(args.This());
            union {
                char bytes[2];
                unsigned char ubytes[2];
            };
            stream->read(bytes, 2);

            if (stream->gcount() < 2)
                throw MLDB::Exception("End of file");
            
            uint16_t u
                = ubytes[1] << 0
                | ubytes[0] << 8;

            args.GetReturnValue().Set(JS::toJS(u));
        } HANDLE_JS_EXCEPTIONS(args);
    }

    static void
    readI16LE(const v8::FunctionCallbackInfo<v8::Value> & args)
    {
        try {
            auto stream = getShared(args.This());
            union {
                char bytes[2];
                unsigned char ubytes[2];
                int16_t u;
            };
            stream->read(bytes, 2);

            if (stream->gcount() < 2)
                throw MLDB::Exception("End of file");
            
            args.GetReturnValue().Set(JS::toJS(u));
        } HANDLE_JS_EXCEPTIONS(args);
    }

    static void
    readI16BE(const v8::FunctionCallbackInfo<v8::Value> & args)
    {
        try {
            auto stream = getShared(args.This());
            union {
                char bytes[2];
                unsigned char ubytes[2];
            };
            stream->read(bytes, 2);

            if (stream->gcount() < 2)
                throw MLDB::Exception("End of file");
            
            int16_t u
                = ubytes[1] << 0
                | ubytes[0] << 8;

            args.GetReturnValue().Set(JS::toJS(u));
        } HANDLE_JS_EXCEPTIONS(args);
    }

    static void
    readBytes(const v8::FunctionCallbackInfo<v8::Value> & args)
    {
        try {
            auto stream = getShared(args.This());
            
            int64_t numBytes = JS::getArg<int64_t>(args, 0, -1, "Number of bytes to load");

            std::vector<unsigned char> bytes;

            if (numBytes == -1) {
                throw MLDB::Exception("no unlimited bytes");
            }
            else {
                bytes.resize(numBytes);
                stream->read((char *)&bytes[0], numBytes);
                bytes.resize(stream->gcount());
            }
            
            args.GetReturnValue().Set(JS::toJS(bytes));
        } HANDLE_JS_EXCEPTIONS(args);
    }

    static void
    eof(const v8::FunctionCallbackInfo<v8::Value> & args)
    {
        try {
            auto stream = getShared(args.This());
            args.GetReturnValue().Set(JS::toJS(stream->eof()));
        } HANDLE_JS_EXCEPTIONS(args);
    }

    static void
    readBlob(const v8::FunctionCallbackInfo<v8::Value> & args)
    {
        try {
            JsContextScope scope(args.This());
            auto stream = getShared(args.This());

            int64_t size = JS::getArg<int64_t>(args, 0, -1, "Number of bytes to load");
            bool allowShort = JS::getArg(args, 1, false, "Allow short reads");

            if (size == -1) {
                // Load all
                std::ostringstream buf;
                buf << stream->rdbuf();

                args.GetReturnValue().Set(JS::toJS(CellValue::blob(buf.str())));
            }
            else {
                // Load from the blob
                std::vector<unsigned char> bytes(size);
                stream->read((char *)&bytes[0], size);

                size_t bytesRead = stream->gcount();
                if (bytesRead != size) {
                    if (!allowShort)
                        throw AnnotatedException(400, "Not enough bytes reading blob and short reads not allowed",
                                                  "bytesRequested", size,
                                                  "bytesAvailable", bytesRead);
                }
                    
                args.GetReturnValue().Set(JS::toJS(CellValue::blob((const char *)&bytes[0],
                                                                   bytesRead)));
            }
            
        } HANDLE_JS_EXCEPTIONS(args);
    }
};

v8::Handle<v8::Object>
StreamJS::
create(std::shared_ptr<std::istream> stream, JsPluginContext * pluginContext)
{
    return doCreateWrapper<StreamJS>(std::move(stream), pluginContext, pluginContext->Stream);
}

std::istream *
StreamJS::
getShared(const v8::Handle<v8::Object> & val)
{
    return reinterpret_cast<StreamJS *>
        (v8::Handle<v8::External>::Cast
         (val->GetInternalField(0))->Value())->stream.get();
}

v8::Local<v8::FunctionTemplate>
StreamJS::
registerMe()
{
    using namespace v8;

    v8::Isolate* isolate = v8::Isolate::GetCurrent();
    EscapableHandleScope scope(isolate);

    auto fntmpl = CreateFunctionTemplate("Stream");
    auto prototmpl = fntmpl->PrototypeTemplate();

#define ADD_METHOD(name) JS::addMethod(isolate, prototmpl, #name, FunctionTemplate::New(isolate, Methods::name))
    ADD_METHOD(readLine);
    ADD_METHOD(readU8);
    ADD_METHOD(readI8);
    ADD_METHOD(readU16LE);
    ADD_METHOD(readU16BE);
    ADD_METHOD(readI16LE);
    ADD_METHOD(readI16BE);
    ADD_METHOD(readU32LE);
    ADD_METHOD(readU32BE);
    ADD_METHOD(readI32LE);
    ADD_METHOD(readI32BE);
    ADD_METHOD(readBytes);
    ADD_METHOD(readJson);
    ADD_METHOD(readBlob);
    ADD_METHOD(eof);
#undef ADD_METHOD

    return scope.Escape(fntmpl);
}


/*****************************************************************************/
/* RANDOM NUMBER GENERATOR                                                   */
/*****************************************************************************/

struct RandomNumberGenerator {
    RandomNumberGenerator(int randomSeed)
        : normal_gen(std::bind(norm, rng)), uniform01(0, 1)
    {
        if (randomSeed)
            rng.seed(randomSeed);

    }

    // Random number support
    mt19937 rng;
    normal_distribution<double> norm;

    std::function<double()> normal_gen;
    std::uniform_real_distribution<> uniform01;

    void seed(int randomSeed)
    {
        rng.seed(randomSeed = 0 ? 1 : randomSeed);
    }

    double uniform(double min = 0.0, double max = 1.0)
    {
        return min + (max - min) * uniform01(rng);
    }

    double normal(double mean = 0.0, double stddev = 1.0)
    {
        return mean + normal_gen() * stddev;
    }
};


/*****************************************************************************/
/* RANDOM NUMBER GENERATOR JS                                                */
/*****************************************************************************/

struct RandomNumberGeneratorJS::Methods {
    static void
    seed(const v8::FunctionCallbackInfo<v8::Value> & args)
    {
        try {
            auto randomNumberGenerator = getShared(args.This());
            int randomSeed = JS::getArg(args, 0, 0, "Integer random seed");
            while (randomSeed == 0)
                randomSeed = random();
            randomNumberGenerator->seed(randomSeed);
            args.GetReturnValue().Set(args.This());
        } HANDLE_JS_EXCEPTIONS(args);
    }

    static void
    normal(const v8::FunctionCallbackInfo<v8::Value> & args)
    {
        try {
            auto randomNumberGenerator = getShared(args.This());
            double mean = JS::getArg(args, 0, 0.0,
                                     "Mean of distribution");
            double stddev = JS::getArg(args, 1, 1.0,
                                       "Standard deviation of distribution");
            args.GetReturnValue().Set(JS::toJS(randomNumberGenerator->normal(mean, stddev)));
        } HANDLE_JS_EXCEPTIONS(args);
    }

    static void
    uniform(const v8::FunctionCallbackInfo<v8::Value> & args)
    {
        try {
            auto randomNumberGenerator = getShared(args.This());
            double min = JS::getArg(args, 0, 0.0,
                                     "Minimum value of distribution");
            double max = JS::getArg(args, 1, 1.0,
                                    "Maximum value of distribution");
            args.GetReturnValue().Set(JS::toJS(randomNumberGenerator->uniform(min, max)));
        } HANDLE_JS_EXCEPTIONS(args);
    }
};

v8::Handle<v8::Object>
RandomNumberGeneratorJS::
create(std::shared_ptr<RandomNumberGenerator> randomNumberGenerator,
       JsPluginContext * pluginContext)
{
    return doCreateWrapper<RandomNumberGeneratorJS>(std::move(randomNumberGenerator), pluginContext, pluginContext->RandomNumberGenerator);
}

RandomNumberGenerator *
RandomNumberGeneratorJS::
getShared(const v8::Handle<v8::Object> & val)
{
    return reinterpret_cast<RandomNumberGeneratorJS *>
        (v8::Handle<v8::External>::Cast
         (val->GetInternalField(0))->Value())->randomNumberGenerator.get();
}

v8::Local<v8::FunctionTemplate>
RandomNumberGeneratorJS::
registerMe()
{
    using namespace v8;

    v8::Isolate* isolate = v8::Isolate::GetCurrent();
    EscapableHandleScope scope(isolate);

    auto fntmpl = CreateFunctionTemplate("RandomNumberGenerator");
    auto prototmpl = fntmpl->PrototypeTemplate();

#define ADD_METHOD(name) JS::addMethod(isolate, prototmpl, #name, FunctionTemplate::New(isolate, Methods::name))
    ADD_METHOD(seed);
    ADD_METHOD(normal);
    ADD_METHOD(uniform);
#undef ADD_METHOD

    return scope.Escape(fntmpl);
}


/*****************************************************************************/
/* MLDB JS                                                                   */
/*****************************************************************************/

struct MldbJS::Methods {

    static void
    openStream(const v8::FunctionCallbackInfo<v8::Value> & args)
    {
        try {
            JsPluginContext * context = MldbJS::getContext(args.This());
            auto stream = std::make_shared<filter_istream>(JS::cstr(args[0]));
            
            args.GetReturnValue().Set(StreamJS::create(stream, context));
        } HANDLE_JS_EXCEPTIONS(args);
    }

    static void
    createDataset(const v8::FunctionCallbackInfo<v8::Value> & args)
    {
        v8::Isolate* isolate = args.GetIsolate();
        try {
            JsPluginContext * context = MldbJS::getContext(args.This());
            MldbEngine * engine = MldbJS::getShared(args.This());
            Json::Value configJson = JS::getArg<Json::Value>(args, 0, "Config");
            PolyConfig config = jsonDecode<PolyConfig>(configJson);

            auto dataset = obtainDataset(engine, config);

            // We may have changed config (id, params, etc).  Modify the
            // input argument to reflect these changes

            auto & configOut = dataset->getConfig();

            auto objectIn = JS::toObject(args[0]);

            JS::set(isolate, objectIn, "id", JS::toJS(configOut.id));
            Json::Value paramsOut = jsonEncode(configOut.params);
            if (paramsOut != configJson["params"]) {
                JS::set(isolate, objectIn, "params", JS::toJS(paramsOut));
            }

            args.GetReturnValue().Set(DatasetJS::create(dataset, context));
        } HANDLE_JS_EXCEPTIONS(args);
    }

    static void
    createFunction(const v8::FunctionCallbackInfo<v8::Value> & args)
    {
        v8::Isolate* isolate = args.GetIsolate();
        try {
            JsPluginContext * context = MldbJS::getContext(args.This());
            MldbEngine * engine = MldbJS::getShared(args.This());
            Json::Value configJson = JS::getArg<Json::Value>(args, 0, "Config");
            PolyConfig config = jsonDecode<PolyConfig>(configJson);

            auto function = obtainFunction(engine, config);

            // We may have changed config (id, params, etc).  Modify the
            // input argument to reflect these changes

            auto & configOut = function->getConfig();

            auto objectIn = JS::toObject(args[0]);

            JS::set(isolate, objectIn, "id", JS::toJS(configOut.id));
            Json::Value paramsOut = jsonEncode(configOut.params);
            if (paramsOut != configJson["params"]) {
                JS::set(isolate, objectIn, "params", JS::toJS(paramsOut));
            }

            args.GetReturnValue().Set(FunctionJS::create(function, context));
        } HANDLE_JS_EXCEPTIONS(args);
    }

    static void
    createSensor(const v8::FunctionCallbackInfo<v8::Value> & args)
    {
        v8::Isolate* isolate = args.GetIsolate();
        try {
            JsPluginContext * context = MldbJS::getContext(args.This());
            MldbEngine * engine = MldbJS::getShared(args.This());
            Json::Value configJson = JS::getArg<Json::Value>(args, 0, "Config");
            PolyConfig config = jsonDecode<PolyConfig>(configJson);

            auto sensor = obtainSensor(engine, config);

            // We may have changed config (id, params, etc).  Modify the
            // input argument to reflect these changes

            auto & configOut = sensor->getConfig();

            auto objectIn = JS::toObject(args[0]);

            JS::set(isolate, objectIn, "id", JS::toJS(configOut.id));
            Json::Value paramsOut = jsonEncode(configOut.params);
            if (paramsOut != configJson["params"]) {
                JS::set(isolate, objectIn, "params", JS::toJS(paramsOut));
            }

            args.GetReturnValue().Set(SensorJS::create(sensor, context));
        } HANDLE_JS_EXCEPTIONS(args);
    }

    static void
    createProcedure(const v8::FunctionCallbackInfo<v8::Value> & args)
    {
        v8::Isolate* isolate = args.GetIsolate();
        try {
            JsPluginContext * context = MldbJS::getContext(args.This());
            MldbEngine * engine = MldbJS::getShared(args.This());
            Json::Value configJson = JS::getArg<Json::Value>(args, 0, "Config");
            PolyConfig config = jsonDecode<PolyConfig>(configJson);

            auto procedure = obtainProcedure(engine, config);

            // We may have changed config (id, params, etc).  Modify the
            // input argument to reflect these changes

            auto & configOut = procedure->getConfig();

            auto objectIn = JS::toObject(args[0]);

            JS::set(isolate, objectIn, "id", JS::toJS(configOut.id));
            Json::Value paramsOut = jsonEncode(configOut.params);
            if (paramsOut != configJson["params"]) {
                JS::set(isolate, objectIn, "params", JS::toJS(paramsOut));
            }

            args.GetReturnValue().Set(ProcedureJS::create(procedure, context));
        } HANDLE_JS_EXCEPTIONS(args);
    }

    static void
    createRandomNumberGenerator(const v8::FunctionCallbackInfo<v8::Value> & args)
    {
        try {
            JsPluginContext * context = MldbJS::getContext(args.This());

            int seed = JS::getArg(args, random(), 0.0, "Seed of generator");
            auto rng = std::make_shared<RandomNumberGenerator>(seed);
            args.GetReturnValue().Set(RandomNumberGeneratorJS::create(rng, context));
        } HANDLE_JS_EXCEPTIONS(args);
    }

    static v8::Handle<v8::Object> doPerform(MldbEngine * engine,
                                            const std::string & verb,
                                            const Utf8String & resource,
                                            const RestParams & params,
                                            Json::Value payload = Json::Value(),
                                            const RestParams & headers = RestParams())
    {
        v8::Isolate* isolate = v8::Isolate::GetCurrent();
        if (payload.isString()) {
            payload = Json::parse(payload.asString());
        }

        HttpHeader header;
        header.verb = verb;
        header.resource = resource.rawString();
        header.queryParams = params;
        for (auto & h: headers)
            header.headers.insert({h.first.toLower().rawString(), h.second.rawString()});


        RestRequest request(header,
                            payload.isNull() ? "" : payload.toStringNoNewLine());

        auto connection = InProcessRestConnection::create();

        engine->handleRequest(*connection, request);

        connection->waitForResponse();

        v8::Handle<v8::Object> result(v8::Object::New(isolate));
        JS::set(isolate, result, "responseCode", JS::toJS(connection->responseCode()));

        if (!connection->contentType().empty())
            JS::set(isolate, result, "contentType", JS::toJS(connection->contentType()));
        if (!connection->headers().empty())
            JS::set(isolate, result, "headers", JS::toJS(connection->headers()));
        if (!connection->response().empty()) {
            JS::set(isolate, result, "response", JS::toJS(connection->response()));
            if (connection->contentType() == "application/json") {
                JS::set(isolate, result, "json", JS::toJS(Json::parse(connection->response())));
            }
        }

        return result;
    }

    static void
    get(const v8::FunctionCallbackInfo<v8::Value> & args)
    {
        v8::Isolate* isolate = args.GetIsolate();
        v8::EscapableHandleScope scope(isolate);
        try {
            MldbEngine * engine = MldbJS::getShared(args.This());
            Utf8String resource = JS::getArg<Utf8String>(args, 0, "resource");

            RestParams params = JS::getArg<RestParams>(args, 1, {}, "params");

            RestParams headers = JS::getArg<RestParams>(args, 2, RestParams(), "headers");

            args.GetReturnValue().Set(scope.Escape(doPerform(engine, "GET", resource, params,
                                                             Json::Value(), headers)));
        } HANDLE_JS_EXCEPTIONS(args);
    }

    static void
    put(const v8::FunctionCallbackInfo<v8::Value> & args)
    {
        v8::Isolate* isolate = args.GetIsolate();
        v8::EscapableHandleScope scope(isolate);
        try {
            MldbEngine * engine = MldbJS::getShared(args.This());
            Utf8String resource = JS::getArg<Utf8String>(args, 0, "resource");
            Json::Value payload = JS::getArg<Json::Value>(args, 1, Json::Value(), "payload");
            RestParams headers = JS::getArg<RestParams>(args, 2, RestParams(), "headers");
            args.GetReturnValue().Set(scope.Escape(doPerform(engine, "PUT", resource, RestParams(), std::move(payload), headers)));
        } HANDLE_JS_EXCEPTIONS(args);
    }

    static void
    putAsync(const v8::FunctionCallbackInfo<v8::Value> & args)
    {
        v8::Isolate* isolate = args.GetIsolate();
        v8::EscapableHandleScope scope(isolate);
        try {
            MldbEngine * engine = MldbJS::getShared(args.This());
            Utf8String resource = JS::getArg<Utf8String>(args, 0, "resource");
            Json::Value payload = JS::getArg<Json::Value>(args, 1, Json::Value(), "payload");
            RestParams headers = JS::getArg<RestParams>(args, 2, RestParams(), "headers");
            headers.emplace_back("async", "true");
            args.GetReturnValue().Set(scope.Escape(doPerform(engine, "PUT", resource, RestParams(), std::move(payload), headers)));
        } HANDLE_JS_EXCEPTIONS(args);
    }

    static void
    post(const v8::FunctionCallbackInfo<v8::Value> & args)
    {
        v8::Isolate* isolate = args.GetIsolate();
        v8::EscapableHandleScope scope(isolate);
        try {
            MldbEngine * engine = MldbJS::getShared(args.This());
            Utf8String resource = JS::getArg<Utf8String>(args, 0, "resource");
            Json::Value payload = JS::getArg<Json::Value>(args, 1, Json::Value(), "payload");
            RestParams headers = JS::getArg<RestParams>(args, 2, RestParams(), "headers");
            args.GetReturnValue().Set(scope.Escape(doPerform(engine, "POST", resource, RestParams(), std::move(payload), headers)));
        } HANDLE_JS_EXCEPTIONS(args);
    }

    static void
    postAsync(const v8::FunctionCallbackInfo<v8::Value> & args)
    {
        v8::Isolate* isolate = args.GetIsolate();
        v8::EscapableHandleScope scope(isolate);
        try {
            MldbEngine * engine = MldbJS::getShared(args.This());
            Utf8String resource = JS::getArg<Utf8String>(args, 0, "resource");
            Json::Value payload = JS::getArg<Json::Value>(args, 1, Json::Value(), "payload");
            RestParams headers = JS::getArg<RestParams>(args, 2, RestParams(), "headers");
            headers.emplace_back("async", "true");
            args.GetReturnValue().Set(scope.Escape(doPerform(engine, "POST", resource, RestParams(), std::move(payload), headers)));
        } HANDLE_JS_EXCEPTIONS(args);
    }

    static void
    del(const v8::FunctionCallbackInfo<v8::Value> & args)
    {
        v8::Isolate* isolate = args.GetIsolate();
        v8::EscapableHandleScope scope(isolate);
        try {
            MldbEngine * engine = MldbJS::getShared(args.This());
            Utf8String resource = JS::getArg<Utf8String>(args, 0, "resource");
            RestParams headers = JS::getArg<RestParams>(args, 1, RestParams(), "headers");

            args.GetReturnValue().Set(scope.Escape(doPerform(engine, "DELETE", resource, RestParams(),
                                                             Json::Value(), headers)));
        } HANDLE_JS_EXCEPTIONS(args);
    }

    static void
    perform(const v8::FunctionCallbackInfo<v8::Value> & args)
    {
        v8::Isolate* isolate = args.GetIsolate();
        v8::EscapableHandleScope scope(isolate);
        try {
            MldbEngine * engine = MldbJS::getShared(args.This());
            string verb = JS::getArg<std::string>(args, 0, "verb");
            Utf8String resource = JS::getArg<Utf8String>(args, 1, "resource");
            RestParams params = JS::getArg<RestParams>(args, 2, {}, "params");
            Json::Value payload = JS::getArg<Json::Value>(args, 3, Json::Value(), "payload");
            RestParams headers = JS::getArg<RestParams>(args, 4, RestParams(), "headers");
            args.GetReturnValue().Set(scope.Escape(doPerform(engine, verb, resource, params, std::move(payload), headers)));
        } HANDLE_JS_EXCEPTIONS(args);
    }

    static void
    diff(const v8::FunctionCallbackInfo<v8::Value> & args)
    {
        v8::Isolate* isolate = args.GetIsolate();
        v8::EscapableHandleScope scope(isolate);
        try {
            Json::Value val1 = JS::getArg<Json::Value>(args, 0, "val1");
            Json::Value val2 = JS::getArg<Json::Value>(args, 1, "val2");
            bool strict = JS::getArg<bool>(args, 2, true, "strict");

            //cerr << "val1 = " << val1 << endl;
            //cerr << "val2 = " << val2 << endl;

            auto diff = jsonDiff(val1, val2, strict);

            //cerr << "diff = " << jsonEncode(diff) << endl;

            auto result = JS::toJS(jsonEncode(diff));

            args.GetReturnValue().Set(scope.Escape(result));
        } HANDLE_JS_EXCEPTIONS(args);
    }
    
    static void
    patch(const v8::FunctionCallbackInfo<v8::Value> & args)
    {
        v8::Isolate* isolate = args.GetIsolate();
        v8::EscapableHandleScope scope(isolate);
        try {
            Json::Value val = JS::getArg<Json::Value>(args, 0, "val");
            Json::Value patch = JS::getArg<Json::Value>(args, 1, "patch");

            auto patched = jsonPatch(val, jsonDecode<JsonDiff>(patch));

            auto result = JS::toJS(jsonEncode(patched));
            
            args.GetReturnValue().Set(scope.Escape(result));
        } HANDLE_JS_EXCEPTIONS(args);
    }

    static void
    ls(const v8::FunctionCallbackInfo<v8::Value> & args)
    {
        v8::Isolate* isolate = args.GetIsolate();
        v8::EscapableHandleScope scope(isolate);
        try {

            string dir = JS::getArg<Utf8String>(args, 0, "dir").rawString();

            std::vector<std::string> dirs;
            std::map<std::string, FsObjectInfo> objects;
            
            auto onSubdir = [&] (const std::string & dirName,
                                 int depth)
                {
                    dirs.push_back(dirName);
                    return false;
                };

            auto onObject = [&] (const std::string & uri,
                                 const FsObjectInfo & info,
                                 const OpenUriObject & open,
                                 int depth)
                {
                    objects[uri] = info;
                    return true;
                };

            forEachUriObject(dir, onObject, onSubdir);

            Json::Value result;
            result["dirs"] = jsonEncode(dirs);
            result["objects"] = jsonEncode(objects);
            
            args.GetReturnValue().Set(scope.Escape(JS::toJS(result)));
        } HANDLE_JS_EXCEPTIONS(args);
    }

    static void
    log(const v8::FunctionCallbackInfo<v8::Value> & args)
    {
        using namespace v8;
        JsPluginContext * context = MldbJS::getContext(args.This());

        try {
            Utf8String line;

            for (unsigned i = 0;  i < args.Length();  ++i) {
                Utf8String printed;

                if (args[i]->IsString() || args[i]->IsStringObject()) {
                    printed = JS::utf8str(args[i]);
                }
                else if (args[i]->IsDate()) {
                    auto isolate = context->isolate.isolate;
                    auto jscontext = context->context.Get(isolate);
                    printed = CellValue(Date::fromSecondsSinceEpoch(JS::check(args[i]->NumberValue(jscontext)) * 0.001)).toUtf8String();
                }
                else if (args[i]->IsObject()) {
                    Json::Value val = JS::fromJS(args[i]);
                    printed = val.toStyledString();
                }
                else if (args[i]->IsArray()) {
                    Json::Value val = JS::fromJS(args[i]);
                    printed = val.toStyledString();
                }
                else {
                    printed = JS::utf8str(args[i]);
                }

                if (i != 0)
                    line += " ";
                line += printed;
            }

            line += "\n";
            
            {
                std::unique_lock<std::mutex> guard(context->logMutex);
                LOG(mldbJsCategory) << line;
                context->logs.emplace_back(Date::now(), "log", Utf8String(line));
            }

            args.GetReturnValue().Set(args.This());
        } HANDLE_JS_EXCEPTIONS(args);
    }

    static void
    createInterval(const v8::FunctionCallbackInfo<v8::Value> & args)
    {
        using namespace v8;
        JsPluginContext * context = MldbJS::getContext(args.This());

        v8::Isolate* isolate = args.GetIsolate();
        v8::EscapableHandleScope scope(isolate);
        try {
            int64_t months = 0;
            int64_t days = 0;
            double seconds = 0;

            if (args.Length() == 1) {
                Json::Value val1 = JS::getArg<Json::Value>(args, 0, "val1");

                if (val1.type() == Json::stringValue) {
                    Json::Value val2;
                    val2["interval"] = val1;
                    CellValue val = jsonDecode<CellValue>(val2);
                    args.GetReturnValue().Set(scope.Escape(CellValueJS::create(val, context)));
                }
                else {
                    for (auto it = val1.begin();  it != val1.end();  ++it) {
                        if (it.memberName() == "months") {
                            months = (*it).asInt();
                        }
                        else if (it.memberName() == "days") {
                            days = (*it).asInt();
                        }
                        else if (it.memberName() == "seconds") {
                            seconds = (*it).asDouble();
                        }
                        else if (it.memberName() == "interval") {
                            ExcAssertEqual(val1.size(), 1);
                            CellValue val = jsonDecode<CellValue>(val1);
                            args.GetReturnValue().Set(scope.Escape(CellValueJS::create(val, context)));
                        }
                        else {
                            throw MLDB::Exception("Unknown object field for interval: '%s'.  Accepted are months, days, seconds or interval by itself");
                        }
                    }
                }
            }
            else {
                months  = JS::getArg<int64_t>(args, 0,   "months");
                days    = JS::getArg<int64_t>(args, 1,   "days");
                seconds = JS::getArg<double>(args,  2,    "seconds");
            }

            CellValue val = CellValue::fromMonthDaySecond(months, days, seconds);
            
            args.GetReturnValue().Set(scope.Escape(CellValueJS::create(val, context)));
        } HANDLE_JS_EXCEPTIONS(args);
    }
    
    static void
    createPath(const v8::FunctionCallbackInfo<v8::Value> & args)
    {
        using namespace v8;
        JsPluginContext * context = MldbJS::getContext(args.This());

        v8::Isolate* isolate = args.GetIsolate();
        v8::EscapableHandleScope scope(isolate);
        try {
            Path val1 = JS::getArg<Path>(args, 0, "argument");
            CellValue val(val1);
            args.GetReturnValue().Set(scope.Escape(CellValueJS::create(val, context)));
        } HANDLE_JS_EXCEPTIONS(args);
    }
    
    static void
    sqlEscape(const v8::FunctionCallbackInfo<v8::Value> & args)
    {
        v8::Isolate* isolate = args.GetIsolate();
        v8::EscapableHandleScope scope(isolate);
        try {
            Utf8String str = JS::getArg<Utf8String>(args, 0, "str");

            auto result = JS::toJS(escapeSql(str));
            
            args.GetReturnValue().Set(scope.Escape(result));
        } HANDLE_JS_EXCEPTIONS(args);
    }

    static void
    query(const v8::FunctionCallbackInfo<v8::Value> & args)
    {
        v8::Isolate* isolate = args.GetIsolate();
        v8::EscapableHandleScope scope(isolate);
        try {
            MldbEngine * engine = MldbJS::getShared(args.This());
            Utf8String query = JS::getArg<Utf8String>(args, 0, "sql");


            auto stm = SelectStatement::parse(query.rawString());
            SqlExpressionMldbScope mldbContext(engine);

            std::vector<MatrixNamedRow> result
                = queryFromStatement(stm, mldbContext, nullptr /*onProgress*/);

            args.GetReturnValue().Set(scope.Escape(JS::toJS(jsonEncode(result))));
        } HANDLE_JS_EXCEPTIONS(args);
    }

    static void
    getHttpBoundAddress(const v8::FunctionCallbackInfo<v8::Value> & args)
    {
        v8::Isolate* isolate = args.GetIsolate();
        v8::EscapableHandleScope scope(isolate);
        try {
            MldbEngine * engine = MldbJS::getShared(args.This());
            args.GetReturnValue().Set(scope.Escape(JS::toJS(engine->getHttpBoundAddress())));
        } HANDLE_JS_EXCEPTIONS(args);
    }

    static void
    debugSetPathOptimizationLevel(const v8::FunctionCallbackInfo<v8::Value> & args)
    {
        using namespace v8;
        try {
            std::string val = JS::getArg<std::string>(args, 0, "level");
            std::string valLc;
            for (auto c: val)
                valLc += tolower(c);
            int level = -1;
            if (valLc == "always") {
                level = OptimizedPath::ALWAYS;
            }
            else if (valLc == "never") {
                level = OptimizedPath::NEVER;
            }
            else if (valLc == "sometimes") {
                level = OptimizedPath::SOMETIMES;
            }
            else throw MLDB::Exception("Couldn't parse path optimization level '"
                                     + val + "': accepted are 'always', 'never' "
                                     "and 'sometimes'");

            OptimizedPath::setDefault(level);
            
            args.GetReturnValue().Set(args.This());
        } HANDLE_JS_EXCEPTIONS(args);
    }
};

MldbEngine *
MldbJS::
getShared(const v8::Handle<v8::Object> & val)
{
    return reinterpret_cast<MldbEngine *>
        (v8::Handle<v8::External>::Cast
         (val->GetInternalField(0))->Value());
}

JsPluginContext *
MldbJS::
getContext(const v8::Handle<v8::Object> & val)
{
    return reinterpret_cast<JsPluginContext *>
        (v8::Handle<v8::External>::Cast
         (val->GetInternalField(1))->Value());
}

v8::Local<v8::ObjectTemplate>
MldbJS::
registerMe()
{
    using namespace v8;

    v8::Isolate* isolate = v8::Isolate::GetCurrent();
    EscapableHandleScope scope(isolate);

    v8::Local<v8::ObjectTemplate> result(ObjectTemplate::New(isolate));

    result->SetInternalFieldCount(2);  // first is mldbEngine, second is plugin cxt

#define ADD_METHOD(name) JS::addMethod(isolate, result, #name, FunctionTemplate::New(isolate, Methods::name))
    ADD_METHOD(log);
    ADD_METHOD(openStream);
    ADD_METHOD(createDataset);
    ADD_METHOD(createFunction);
    ADD_METHOD(createSensor);
    ADD_METHOD(createProcedure);
    ADD_METHOD(createInterval);
    ADD_METHOD(createPath);
    ADD_METHOD(createRandomNumberGenerator);
    ADD_METHOD(perform);
    ADD_METHOD(get);
    ADD_METHOD(put);
    ADD_METHOD(putAsync);
    ADD_METHOD(post);
    ADD_METHOD(postAsync);
    ADD_METHOD(del);
    ADD_METHOD(diff);
    ADD_METHOD(patch);
    ADD_METHOD(query);
    ADD_METHOD(sqlEscape);
    ADD_METHOD(patch);
    ADD_METHOD(ls);
    ADD_METHOD(getHttpBoundAddress);
    ADD_METHOD(debugSetPathOptimizationLevel);
#undef ADD_METHOD
    

    return scope.Escape(result);
}


} // namespace MLDB


