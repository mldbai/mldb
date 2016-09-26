/** mldb_js.cc
    Jeremy Barnes, 20 June 2015
    Copyright (c) 2015 mldb.ai inc.  All rights reserved.

    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

    JS bindings for MLDB.
*/

#include "mldb_js.h"
#include "mldb/types/basic_value_descriptions.h"
#include "mldb/rest/in_process_rest_connection.h"
#include "mldb/core/dataset.h"
#include "mldb/core/procedure.h"
#include "mldb/core/function.h"
#include "mldb/core/sensor.h"
#include "mldb/base/optimized_path.h"
#include "procedure_js.h"
#include "function_js.h"
#include "sensor_js.h"
#include "dataset_js.h"
#include "mldb/server/mldb_server.h"
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
create(CellValue value, JsPluginContext * context)
{
    v8::Isolate* isolate = v8::Isolate::GetCurrent();
    auto obj = context->CellValue.Get(isolate)->GetFunction()->NewInstance();
    auto * wrapped = new CellValueJS();
    wrapped->val = std::move(value);
    wrapped->wrap(obj, context);
    return obj;
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
                        throw HttpReturnException(400, "Not enough bytes reading blob and short reads not allowed",
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
create(std::shared_ptr<std::istream> stream, JsPluginContext * context)
{
    v8::Isolate* isolate = v8::Isolate::GetCurrent();
    auto obj = context->Stream.Get(isolate)->GetFunction()->NewInstance();
    auto * wrapped = new StreamJS();
    wrapped->stream = stream;
    wrapped->wrap(obj, context);
    return obj;
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
    auto objtmpl = fntmpl->InstanceTemplate();

    objtmpl->Set(String::NewFromUtf8(isolate, "readLine"),
                 FunctionTemplate::New(isolate, Methods::readLine));

    objtmpl->Set(String::NewFromUtf8(isolate, "readU8"),
                 FunctionTemplate::New(isolate, Methods::readU8));
    objtmpl->Set(String::NewFromUtf8(isolate, "readI8"),
                 FunctionTemplate::New(isolate, Methods::readI8));

    objtmpl->Set(String::NewFromUtf8(isolate, "readU16LE"),
                 FunctionTemplate::New(isolate, Methods::readU16LE));
    objtmpl->Set(String::NewFromUtf8(isolate, "readU16BE"),
                 FunctionTemplate::New(isolate, Methods::readU16BE));
    objtmpl->Set(String::NewFromUtf8(isolate, "readI16LE"),
                 FunctionTemplate::New(isolate, Methods::readI16LE));
    objtmpl->Set(String::NewFromUtf8(isolate, "readI16BE"),
                 FunctionTemplate::New(isolate, Methods::readI16BE));

    objtmpl->Set(String::NewFromUtf8(isolate, "readU32LE"),
                 FunctionTemplate::New(isolate, Methods::readU32LE));
    objtmpl->Set(String::NewFromUtf8(isolate, "readU32BE"),
                 FunctionTemplate::New(isolate, Methods::readU32BE));
    objtmpl->Set(String::NewFromUtf8(isolate, "readI32LE"),
                 FunctionTemplate::New(isolate, Methods::readI32LE));
    objtmpl->Set(String::NewFromUtf8(isolate, "readI32BE"),
                 FunctionTemplate::New(isolate, Methods::readI32BE));

    objtmpl->Set(String::NewFromUtf8(isolate, "readBytes"),
                 FunctionTemplate::New(isolate, Methods::readBytes));
    objtmpl->Set(String::NewFromUtf8(isolate, "readJson"),
                 FunctionTemplate::New(isolate, Methods::readJson));
    objtmpl->Set(String::NewFromUtf8(isolate, "readBlob"),
                 FunctionTemplate::New(isolate, Methods::readBlob));

    objtmpl->Set(String::NewFromUtf8(isolate, "eof"),
                 FunctionTemplate::New(isolate, Methods::eof));
        

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
       JsPluginContext * context)
{
    v8::Isolate* isolate = v8::Isolate::GetCurrent();
    auto obj = context->RandomNumberGenerator.Get(isolate)
        ->GetFunction()->NewInstance();
    auto * wrapped = new RandomNumberGeneratorJS();
    wrapped->randomNumberGenerator = randomNumberGenerator;
    wrapped->wrap(obj, context);
    return obj;
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
    auto objtmpl = fntmpl->InstanceTemplate();

    objtmpl->Set(String::NewFromUtf8(isolate, "seed"),
                 FunctionTemplate::New(isolate, Methods::seed));
    objtmpl->Set(String::NewFromUtf8(isolate, "normal"),
                 FunctionTemplate::New(isolate, Methods::normal));
    objtmpl->Set(String::NewFromUtf8(isolate, "uniform"),
                 FunctionTemplate::New(isolate, Methods::uniform));

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
            MldbServer * server = MldbJS::getShared(args.This());
            Json::Value configJson = JS::getArg<Json::Value>(args, 0, "Config");
            PolyConfig config = jsonDecode<PolyConfig>(configJson);

            auto dataset = obtainDataset(server, config);

            // We may have changed config (id, params, etc).  Modify the
            // input argument to reflect these changes

            auto & configOut = dataset->getConfig();

            auto objectIn = JS::toObject(args[0]);

            objectIn->Set(v8::String::NewFromUtf8(isolate, "id"),
                          JS::toJS(configOut.id));
            Json::Value paramsOut = jsonEncode(configOut.params);
            if (paramsOut != configJson["params"]) {
                objectIn->Set(v8::String::NewFromUtf8(isolate, "params"),
                              JS::toJS(paramsOut));
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
            MldbServer * server = MldbJS::getShared(args.This());
            Json::Value configJson = JS::getArg<Json::Value>(args, 0, "Config");
            PolyConfig config = jsonDecode<PolyConfig>(configJson);

            auto function = obtainFunction(server, config);

            // We may have changed config (id, params, etc).  Modify the
            // input argument to reflect these changes

            auto & configOut = function->getConfig();

            auto objectIn = JS::toObject(args[0]);

            objectIn->Set(v8::String::NewFromUtf8(isolate, "id"), JS::toJS(configOut.id));
            Json::Value paramsOut = jsonEncode(configOut.params);
            if (paramsOut != configJson["params"]) {
                objectIn->Set(v8::String::NewFromUtf8(isolate, "params"), JS::toJS(paramsOut));
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
            MldbServer * server = MldbJS::getShared(args.This());
            Json::Value configJson = JS::getArg<Json::Value>(args, 0, "Config");
            PolyConfig config = jsonDecode<PolyConfig>(configJson);

            auto sensor = obtainSensor(server, config);

            // We may have changed config (id, params, etc).  Modify the
            // input argument to reflect these changes

            auto & configOut = sensor->getConfig();

            auto objectIn = JS::toObject(args[0]);

            objectIn->Set(v8::String::NewFromUtf8(isolate, "id"), JS::toJS(configOut.id));
            Json::Value paramsOut = jsonEncode(configOut.params);
            if (paramsOut != configJson["params"]) {
                objectIn->Set(v8::String::NewFromUtf8(isolate, "params"), JS::toJS(paramsOut));
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
            MldbServer * server = MldbJS::getShared(args.This());
            Json::Value configJson = JS::getArg<Json::Value>(args, 0, "Config");
            PolyConfig config = jsonDecode<PolyConfig>(configJson);

            auto procedure = obtainProcedure(server, config);

            // We may have changed config (id, params, etc).  Modify the
            // input argument to reflect these changes

            auto & configOut = procedure->getConfig();

            auto objectIn = JS::toObject(args[0]);

            objectIn->Set(v8::String::NewFromUtf8(isolate, "id"), JS::toJS(configOut.id));
            Json::Value paramsOut = jsonEncode(configOut.params);
            if (paramsOut != configJson["params"]) {
                objectIn->Set(v8::String::NewFromUtf8(isolate, "params"), JS::toJS(paramsOut));
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

    static v8::Handle<v8::Object> doPerform(MldbServer * server,
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

        InProcessRestConnection connection;

        server->handleRequest(connection, request);

        v8::Handle<v8::Object> result(v8::Object::New(isolate));
        result->Set(v8::String::NewFromUtf8(isolate, "responseCode"),
                    JS::toJS(connection.responseCode));

        if (!connection.contentType.empty())
            result->Set(v8::String::NewFromUtf8(isolate, "contentType"),
                        JS::toJS(connection.contentType));
        if (!connection.headers.empty())
            result->Set(v8::String::NewFromUtf8(isolate, "headers"),
                        JS::toJS(connection.headers));
        if (!connection.response.empty()) {
            result->Set(v8::String::NewFromUtf8(isolate, "response"),
                        JS::toJS(connection.response));
            if (connection.contentType == "application/json") {
                result->Set(v8::String::NewFromUtf8(isolate, "json"),
                            JS::toJS(Json::parse(connection.response)));
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
            MldbServer * server = MldbJS::getShared(args.This());
            Utf8String resource = JS::getArg<Utf8String>(args, 0, "resource");

            RestParams params = JS::getArg<RestParams>(args, 1, {}, "params");

            RestParams headers = JS::getArg<RestParams>(args, 2, RestParams(), "headers");

            args.GetReturnValue().Set(scope.Escape(doPerform(server, "GET", resource, params,
                                                             Json::Value(), headers)));
        } HANDLE_JS_EXCEPTIONS(args);
    }

    static void
    put(const v8::FunctionCallbackInfo<v8::Value> & args)
    {
        v8::Isolate* isolate = args.GetIsolate();
        v8::EscapableHandleScope scope(isolate);
        try {
            MldbServer * server = MldbJS::getShared(args.This());
            Utf8String resource = JS::getArg<Utf8String>(args, 0, "resource");
            Json::Value payload = JS::getArg<Json::Value>(args, 1, Json::Value(), "payload");
            RestParams headers = JS::getArg<RestParams>(args, 2, RestParams(), "headers");
            args.GetReturnValue().Set(scope.Escape(doPerform(server, "PUT", resource, RestParams(), std::move(payload), headers)));
        } HANDLE_JS_EXCEPTIONS(args);
    }

    static void
    putAsync(const v8::FunctionCallbackInfo<v8::Value> & args)
    {
        v8::Isolate* isolate = args.GetIsolate();
        v8::EscapableHandleScope scope(isolate);
        try {
            MldbServer * server = MldbJS::getShared(args.This());
            Utf8String resource = JS::getArg<Utf8String>(args, 0, "resource");
            Json::Value payload = JS::getArg<Json::Value>(args, 1, Json::Value(), "payload");
            RestParams headers = JS::getArg<RestParams>(args, 2, RestParams(), "headers");
            headers.emplace_back("async", "true");
            args.GetReturnValue().Set(scope.Escape(doPerform(server, "PUT", resource, RestParams(), std::move(payload), headers)));
        } HANDLE_JS_EXCEPTIONS(args);
    }

    static void
    post(const v8::FunctionCallbackInfo<v8::Value> & args)
    {
        v8::Isolate* isolate = args.GetIsolate();
        v8::EscapableHandleScope scope(isolate);
        try {
            MldbServer * server = MldbJS::getShared(args.This());
            Utf8String resource = JS::getArg<Utf8String>(args, 0, "resource");
            Json::Value payload = JS::getArg<Json::Value>(args, 1, Json::Value(), "payload");
            RestParams headers = JS::getArg<RestParams>(args, 2, RestParams(), "headers");
            args.GetReturnValue().Set(scope.Escape(doPerform(server, "POST", resource, RestParams(), std::move(payload), headers)));
        } HANDLE_JS_EXCEPTIONS(args);
    }

    static void
    postAsync(const v8::FunctionCallbackInfo<v8::Value> & args)
    {
        v8::Isolate* isolate = args.GetIsolate();
        v8::EscapableHandleScope scope(isolate);
        try {
            MldbServer * server = MldbJS::getShared(args.This());
            Utf8String resource = JS::getArg<Utf8String>(args, 0, "resource");
            Json::Value payload = JS::getArg<Json::Value>(args, 1, Json::Value(), "payload");
            RestParams headers = JS::getArg<RestParams>(args, 2, RestParams(), "headers");
            headers.emplace_back("async", "true");
            args.GetReturnValue().Set(scope.Escape(doPerform(server, "POST", resource, RestParams(), std::move(payload), headers)));
        } HANDLE_JS_EXCEPTIONS(args);
    }

    static void
    del(const v8::FunctionCallbackInfo<v8::Value> & args)
    {
        v8::Isolate* isolate = args.GetIsolate();
        v8::EscapableHandleScope scope(isolate);
        try {
            MldbServer * server = MldbJS::getShared(args.This());
            Utf8String resource = JS::getArg<Utf8String>(args, 0, "resource");
            RestParams headers = JS::getArg<RestParams>(args, 1, RestParams(), "headers");

            args.GetReturnValue().Set(scope.Escape(doPerform(server, "DELETE", resource, RestParams(),
                                                             Json::Value(), headers)));
        } HANDLE_JS_EXCEPTIONS(args);
    }

    static void
    perform(const v8::FunctionCallbackInfo<v8::Value> & args)
    {
        v8::Isolate* isolate = args.GetIsolate();
        v8::EscapableHandleScope scope(isolate);
        try {
            MldbServer * server = MldbJS::getShared(args.This());
            string verb = JS::getArg<std::string>(args, 0, "verb");
            Utf8String resource = JS::getArg<Utf8String>(args, 1, "resource");
            RestParams params = JS::getArg<RestParams>(args, 2, {}, "params");
            Json::Value payload = JS::getArg<Json::Value>(args, 3, Json::Value(), "payload");
            RestParams headers = JS::getArg<RestParams>(args, 4, RestParams(), "headers");
            args.GetReturnValue().Set(scope.Escape(doPerform(server, verb, resource, params, std::move(payload), headers)));
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
                    printed = CellValue(Date::fromSecondsSinceEpoch(args[i]->NumberValue() * 0.001)).toUtf8String();
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
            MldbServer * server = MldbJS::getShared(args.This());
            Utf8String query = JS::getArg<Utf8String>(args, 0, "sql");
            std::vector<MatrixNamedRow> result = server->query(query);

            args.GetReturnValue().Set(scope.Escape(JS::toJS(jsonEncode(result))));
        } HANDLE_JS_EXCEPTIONS(args);
    }

    static void
    getHttpBoundAddress(const v8::FunctionCallbackInfo<v8::Value> & args)
    {
        v8::Isolate* isolate = args.GetIsolate();
        v8::EscapableHandleScope scope(isolate);
        try {
            MldbServer * server = MldbJS::getShared(args.This());
            args.GetReturnValue().Set(scope.Escape(JS::toJS(server->httpBoundAddress)));
        } HANDLE_JS_EXCEPTIONS(args);
    }

    static void
    setPathOptimizationLevel(const v8::FunctionCallbackInfo<v8::Value> & args)
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

MldbServer *
MldbJS::
getShared(const v8::Handle<v8::Object> & val)
{
    return reinterpret_cast<MldbServer *>
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

    v8::Local<v8::ObjectTemplate> result(ObjectTemplate::New());

    result->SetInternalFieldCount(2);  // first is mldbServer, second is plugin cxt

    result->Set(String::NewFromUtf8(isolate, "log"), FunctionTemplate::New(isolate, Methods::log));
    result->Set(String::NewFromUtf8(isolate, "openStream"),
                FunctionTemplate::New(isolate, Methods::openStream));
    result->Set(String::NewFromUtf8(isolate, "createDataset"),
                FunctionTemplate::New(isolate, Methods::createDataset));
    result->Set(String::NewFromUtf8(isolate, "createFunction"),
                FunctionTemplate::New(isolate, Methods::createFunction));
    result->Set(String::NewFromUtf8(isolate, "createSensor"),
                FunctionTemplate::New(isolate, Methods::createSensor));
    result->Set(String::NewFromUtf8(isolate, "createProcedure"),
                FunctionTemplate::New(isolate, Methods::createProcedure));
    result->Set(String::NewFromUtf8(isolate, "createInterval"),
                FunctionTemplate::New(isolate, Methods::createInterval));
    result->Set(String::NewFromUtf8(isolate, "createPath"),
                FunctionTemplate::New(isolate, Methods::createPath));
    result->Set(String::NewFromUtf8(isolate, "createRandomNumberGenerator"),
                FunctionTemplate::New(isolate, Methods::createRandomNumberGenerator));
    result->Set(String::NewFromUtf8(isolate, "perform"),
                FunctionTemplate::New(isolate, Methods::perform));
    result->Set(String::NewFromUtf8(isolate, "get"),
                FunctionTemplate::New(isolate, Methods::get));
    result->Set(String::NewFromUtf8(isolate, "put"),
                FunctionTemplate::New(isolate, Methods::put));
    result->Set(String::NewFromUtf8(isolate, "putAsync"),
                FunctionTemplate::New(isolate, Methods::putAsync));
    result->Set(String::NewFromUtf8(isolate, "post"),
                FunctionTemplate::New(isolate, Methods::post));
    result->Set(String::NewFromUtf8(isolate, "postAsync"),
                FunctionTemplate::New(isolate, Methods::postAsync));
    result->Set(String::NewFromUtf8(isolate, "del"),
                FunctionTemplate::New(isolate, Methods::del));

    result->Set(String::NewFromUtf8(isolate, "diff"),
                FunctionTemplate::New(isolate, Methods::diff));
    result->Set(String::NewFromUtf8(isolate, "patch"),
                FunctionTemplate::New(isolate, Methods::patch));

    result->Set(String::NewFromUtf8(isolate, "query"),
                FunctionTemplate::New(isolate, Methods::query));
    result->Set(String::NewFromUtf8(isolate, "sqlEscape"),
                FunctionTemplate::New(isolate, Methods::sqlEscape));

    result->Set(String::NewFromUtf8(isolate, "ls"),
                FunctionTemplate::New(isolate, Methods::ls));
    result->Set(String::NewFromUtf8(isolate, "getHttpBoundAddress"),
                FunctionTemplate::New(isolate, Methods::getHttpBoundAddress));

    result->Set(String::NewFromUtf8(isolate, "debugSetPathOptimizationLevel"),
                FunctionTemplate::New(isolate, Methods::setPathOptimizationLevel));

    return scope.Escape(result);
}

void
MldbJS::
New(const v8::FunctionCallbackInfo<v8::Value> & args)
{
    try {
        throw MLDB::Exception("can't create new JS plugins");
    } HANDLE_JS_EXCEPTIONS(args);
}

} // namespace MLDB


