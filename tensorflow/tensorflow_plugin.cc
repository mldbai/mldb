/** tensorflow_plugin.cc
    Jeremy Barnes, 24 November 2015
    Copyright (c) 2015 mldb.ai inc.  All rights reserved.

*/

#include <thread>

#include "mldb/core/mldb_entity.h"
#include "mldb/core/function.h"
#include "mldb/core/plugin.h"
#include "mldb/types/structure_description.h"
#include "mldb/types/vector_description.h"
#include "mldb/types/enum_description.h"
#include "mldb/types/map_description.h"
#include "mldb/types/url.h"
#include "mldb/types/any_impl.h"
#include "mldb/arch/timers.h"
#include "mldb/sql/binding_contexts.h"
#include "mldb/base/parallel.h"
#include "mldb/jml/utils/string_functions.h"
#include "mldb/vfs/filter_streams.h"
#include "mldb/vfs/fs_utils.h"
#include "mldb/server/dataset_context.h"
#include "mldb/rest/rest_request_binding.h"
#include "mldb/server/static_content_macro.h"
#include "mldb/sql/builtin_functions.h"
#include "mldb/utils/log.h"

#include "google/protobuf/util/json_util.h"
#include "google/protobuf/util/type_resolver_util.h"
#include "google/protobuf/io/zero_copy_stream_impl.h"
#include "tensorflow/core/framework/op.h"
#include "tensorflow/core/framework/op_def.pb.h"
#include "tensorflow/core/platform/init_main.h"
#include "tensorflow/core/framework/graph.pb.h"
#include "tensorflow/core/public/session.h"
#include "tensorflow/core/common_runtime/device_factory.h"
#include "tensorflow/core/common_runtime/device.h"

#include "tensorflow/cc/ops/const_op.h"
#include "tensorflow/cc/ops/image_ops.h"
#include "tensorflow/cc/ops/standard_ops.h"
#include "tensorflow/core/framework/tensor.h"
#include "tensorflow/core/graph/default_device.h"
#include "tensorflow/core/graph/graph_def_builder.h"
#include "tensorflow/core/lib/core/errors.h"
#include "tensorflow/core/lib/core/stringpiece.h"
#include "tensorflow/core/lib/core/threadpool.h"
#include "tensorflow/core/lib/io/path.h"
#include "tensorflow/core/lib/strings/stringprintf.h"
#include "tensorflow/core/platform/logging.h"
#include "tensorflow/core/framework/graph_def_util.h"
#include "tensorflow/core/graph/default_device.h"
#include "tensorflow/core/platform/tracing.h"

using namespace std;

namespace tensorflow {
#define DEFINE_ENUM_DESCRIPTION_PROTO(Name, Type) \
    struct Name                                                     \
        : public MLDB::EnumDescription<Type> {                      \
        Name();                                                     \
    };                                                              \
                                                                    \
    MLDB::ValueDescriptionT<Type> *                                 \
    getDefaultDescription(Type *)                        \
    {                                                               \
        return new Name();                                          \
    }                                                               \
                                                                    \
    MLDB::ValueDescriptionT<Type> *                                 \
    getDefaultDescriptionUninitialized(Type *)           \
    {                                                               \
        return new Name();                                          \
    }                                                               \
                                                                    \
    Name::Name()                                                    \
    {                                                               \
        auto desc = Type##_descriptor();                 \
        google::protobuf::DebugStringOptions options;               \
        options.include_comments = true;                            \
        for (unsigned i = 0;  i < desc->value_count();  ++i) {      \
            addValue(desc->value(i)->name(),                        \
                     (Type)desc->value(i)->number(),     \
                     desc->value(i)->DebugStringWithOptions(options));\
        }                                                           \
    }


DECLARE_ENUM_DESCRIPTION_NAMED(TensorflowDataTypeDescription, tensorflow::DataType);
DEFINE_ENUM_DESCRIPTION_PROTO(TensorflowDataTypeDescription, DataType);

#if 0
DECLARE_STRUCTURE_DESCRIPTION_NAMED(TensorflowAttrValueDescription,
                                    tensorflow::AttrValue);
DEFINE_STRUCTURE_DESCRIPTION_NAMED(TensorflowAttrValueDescription,
                                   tensorflow::AttrValue);

TensorflowAttrValueDescription::
TensorflowAttrValueDescription()
{    
    using namespace MLDB;

    const ::google::protobuf::Descriptor * desc
        = tensorflow::AttrValue::descriptor();

    onUnknownField = [=] (tensorflow::AttrValue * val,
                          JsonParsingContext & context)
        {
            if (context.fieldName() == "type") {
                Json::Value j = context.expectJson();
                val->set_type(jsonDecode<tensorflow::DataType>(j));
                return;
            }
            throw HttpReturnException(400, "Unknown field '" + context.fieldName()
                                      + "' parsing Tensorflow AttrValue");
        };
}
#endif

struct AttrValueDescription: public MLDB::ValueDescriptionT<AttrValue> {
    virtual void parseJsonTyped(AttrValue * val,
                                MLDB::JsonParsingContext & context) const
    {
        using namespace MLDB;

        if (context.isNull()) {
            context.expectNull();
            *val = AttrValue();
        }
        else if(context.isString())
            val->set_s(context.expectStringUtf8().rawString());
        else if (context.isInt())
            val->set_i(context.expectLongLong());
        else if (context.isNumber())
            val->set_f(context.expectDouble());
        else if (context.isBool())
            val->set_b(context.expectBool());
        else if (context.isObject()) {
            Json::Value v = context.expectJson();
            ExcAssertEqual(v.size(), 1);
            if (v.isMember("type")) {
                val->set_type(jsonDecode<tensorflow::DataType>(v["type"]));
            }
            else if (v.isMember("shape")) {
                throw HttpReturnException(500, "shape attributes not done");
            }
            else if (v.isMember("tensor")) {
                throw HttpReturnException(500, "tensor attributes not done");
            }
            else if (v.isMember("list")) {
                throw HttpReturnException(500, "list attributes not done");
            }
            else if (v.isMember("func")) {
                throw HttpReturnException(500, "func attributes not done");
            }
            else if (v.isMember("placeholder")) {
                throw HttpReturnException(500, "placeholder attributes not done");
            }
            else {
                throw HttpReturnException(400, "Unknown JSON AttrValue '" + v.toStringNoNewLine() + "'");
            }
        }
        else if (context.isArray()) {
            // Do a list if singly nested
            Json::Value v = context.expectJson();

            if (v.empty()) {
                val->mutable_list();
            }
            else {
                switch (v[0].type()) {
                case Json::nullValue:
                    throw HttpReturnException(500, "Can't convert JSON NULL to a TensorFlow attribute value");
                case Json::intValue:
                case Json::uintValue: {
                    auto * l = val->mutable_list();
                    for (auto & jval: v) {
                        l->add_i(jval.asInt());
                    }
                    return;
                }
                case Json::realValue:  {
                    auto * l = val->mutable_list();
                    for (auto & jval: v) {
                        l->add_f(jval.asDouble());
                    }
                    return;
                }
                case Json::stringValue:  {
                    auto * l = val->mutable_list();
                    for (auto & jval: v) {
                        l->add_s(jval.asString());
                    }
                    return;
                }
                case Json::booleanValue:  {
                    auto * l = val->mutable_list();
                    for (auto & jval: v) {
                        l->add_b(jval.asBool());
                    }
                    return;
                }
                case Json::arrayValue:
                case Json::objectValue:
                    throw HttpReturnException(400, "Can't do TensorFlow list of structured types to attr");
                }
            }
        }
        else {
            Json::Value val = context.expectJson();
            cerr << "val = " << val << endl;
            throw HttpReturnException(400, "Unknown Tensorflow attribute value",
                                      "json",
                                      val.toStringNoNewLine());
        }
    }
    
    virtual void printJsonTyped(const AttrValue * val,
                                MLDB::JsonPrintingContext & context) const
    {
        using namespace MLDB;

        switch (val->value_case()) {
        case tensorflow::AttrValue::kS:
            context.writeStringUtf8(Utf8String(val->s()));
            return;
        case tensorflow::AttrValue::kI:
            context.writeLongLong(val->i());
            return;
        case tensorflow::AttrValue::kF:
            context.writeDouble(val->f());
            return;
        case tensorflow::AttrValue::kB:
            context.writeBool(val->b());
            return;
        case tensorflow::AttrValue::kType:
            context.writeJson(jsonEncode(val->type()));
            return;
        case tensorflow::AttrValue::kShape:
        case tensorflow::AttrValue::kTensor:
        case tensorflow::AttrValue::kList:
        case tensorflow::AttrValue::kFunc:
        case tensorflow::AttrValue::kPlaceholder:
        case tensorflow::AttrValue::VALUE_NOT_SET:
            context.writeNull();
            return;
        }

        throw HttpReturnException(500, "Attribute value not printable");
    }

    virtual bool isDefaultTyped(const AttrValue * val) const
    {
        return val->value_case() == tensorflow::AttrValue::VALUE_NOT_SET;
    }
};

DEFINE_VALUE_DESCRIPTION_NS(AttrValue, AttrValueDescription);

} // namespace tensorflow


namespace MLDB {

static Json::Value protoToJson(const ::google::protobuf::Message & obj)
{
    using namespace google::protobuf;
    using namespace google::protobuf::util;

    std::string type_url = "/" + obj.GetDescriptor()->full_name();
    std::string bin;
    obj.SerializeToString(&bin);
    std::string jstring;

    std::string url_prefix;

    TypeResolver* resolver
        = NewTypeResolverForDescriptorPool
        (url_prefix, DescriptorPool::generated_pool());
        
    //cerr << "resolver = " << resolver << endl;

    JsonOptions options;
    options.add_whitespace = true;

    auto res = BinaryToJsonString(resolver,
                                  type_url,
                                  bin,
                                  &jstring,
                                  options);
    

    if (!res.ok()) {
        throw HttpReturnException
            (500, "Couldn't initialize tensorflow graph model: "
             + res.error_message());
    }

    return Json::parse(jstring);

}

const Package & tensorflowPackage()
{
    static const Package result("tensorflow");
    return result;
}

/*****************************************************************************/
/* TENSORFLOW GRAPH BASE                                                     */
/*****************************************************************************/

struct TensorflowGraphBase: public Function {

    TensorflowGraphBase(MldbServer * owner, PolyConfig config)
        : Function(owner, config)
    {
    }

    SelectExpression inputs;
    SelectExpression outputs;

    void init(std::unique_ptr<tensorflow::GraphDef> graphIn,
              SelectExpression inputs_,
              SelectExpression outputs_,
              const Regex & deviceFilter)
    {
        using namespace tensorflow;

        ExcAssert(graphIn.get());
        this->graph = std::move(graphIn);

        inputs = std::move(inputs_);
        outputs = std::move(outputs_);

        // Those without a device set can be bound to multiple
        // places.
        std::set<std::string> nodesWithoutDevices;

        // These nodes have constants which we shouldn't need to spend
        // time running one after the other

        size_t constantTotalBytes = 0;

        // Tell it not to use cudnn, since it's not necessarily available
        for (auto & node: *graph->mutable_node()) {
            //cerr << node.DebugString() << endl;
            //cerr << "found node " << node.name() << endl;
            //cerr << "op = " << node.op() << endl;
            //cerr << "device = " << node.device() << endl;
            //cerr << "dtype = " << node.attr().find("dtype")->second.DebugString() << endl;
            if (node.op() == "Const") {
                auto it = node.attr().find("value");
                if (it == node.attr().end())
                    throw HttpReturnException(500, "Const with no value");

                Tensor tensor;
                if (!tensor.FromProto(it->second.tensor()))
                    throw HttpReturnException(500, "Const is not parseable");
                
                constantTotalBytes += tensor.TotalBytes();

                // Returned the constant
                constants[node.name()] = std::move(tensor);
            }

            if (node.device().empty())
                nodesWithoutDevices.insert(node.name());

            for (auto & attr: *node.mutable_attr()) {
                if (attr.first == "value")
                    continue;
                //cerr << "attr " << attr.first << " " << attr.second.DebugString()
                //<< endl;

                // TODO: interrogate to see if cudnn is available
                //if (attr.first == "use_cudnn_on_gpu") {
                //    attr.second.set_b(false);
                //}
            }
            //for (const auto & input: node.input()) {
            //    cerr << "input " << input << endl;
            //}
        }

        // Set them up as a list
        //this->constants.insert(this->constants.end(),
        //                       constants.begin(),
        //                       constants.end());

        tensorflow::SessionOptions options;
        // Allow it to use all GPUs
        options.config.mutable_device_count()->insert({"GPU", 128});

        // As well as the CPU
        options.config.mutable_device_count()->insert({"CPU", 1});
        //options.config.set_log_device_placement(true);

        // We need to create a session for each device, unfortunately,
        // due to Tensorflow having a "first matching" policy to
        // allocate devices to an executing graph.
        std::vector<::tensorflow::Device *> devices;
        tensorflow::DeviceFactory::AddDevices(options, "", &devices);
        
        int sessionsPerDevice = 1;

        // Operations hardcoded to the CPU.  These are those that
        // can't use GPUs or need large amounts of data to be
        // transferred back and forth and so don't make sense.
        set<string> hardcodedCpu = {
            /*"ExpandDims", "ResizeBilinear"*/ /*, "Cast", "Sub", "Mul", "ExpandDims/dim"*/ };

        int numMatchedDevices = 0;

        for (const auto & d: devices) {
            std::string deviceName = d->name();

            if (!regex_match(deviceName, deviceFilter)) {
                continue;
            }

            ++numMatchedDevices;

            bool isCpuDevice = deviceName.find("/cpu:") != std::string::npos;

            // Set the device for all nodes where it's not hardcoded
            for (auto & node: *graph->mutable_node()) {
                if (nodesWithoutDevices.count(node.name())) {
                    bool isCpu = false;
                    for (const auto & c: hardcodedCpu) {
                        if (node.name().find(c) == 0)
                            isCpu = true;
                    }

                    //auto it = hardcodedCpu.lower_bound(node.name());
                    //if (it != hardcodedCpu.end() && node.name().find(*it) == 0)
                    //isCpu = false;//true;
                    if (isCpu) {
                        cerr << "node " << node.name() << " runs on CPU" << endl;
                        node.set_device("/cpu:0");
                    }
                    else {
                        node.set_device(d->name());
                    }
                }
            }

            if (isCpuDevice) {
                // Use the thread pool for CPU threads
                //options.config.set_use_per_session_threads(true);
                //options.config.set_inter_op_parallelism_threads(2);
            }
            else {
                // The GPU gets some CPU threads of its own so that
                // CPU operations can't block GPU operations.  This
                // is necessary to achieve maximum occupancy of the
                // GPU.
                options.config.set_allow_soft_placement(true);
                //options.config.set_use_per_session_threads(true);
                options.config.set_inter_op_parallelism_threads(4);
            }

            // NOTE: eventually, this will work... but until it does, we
            // need to go through the above.  Currently Tensorflow just
            // ignores the device_filters fields.
            options.config.add_device_filters(d->name());

            for (unsigned i = 0;  i < sessionsPerDevice;  ++i) {
                std::unique_ptr<tensorflow::Session> session;
                session.reset(tensorflow::NewSession(options));
                Status session_create_status = session->Create(*graph);
                
                if (!session_create_status.ok()) {
                    throw HttpReturnException
                        (500, "Couldn't initialize tensorflow graph model: "
                         + session_create_status.error_message());
                }
                
                sessions.emplace_back(d->name(), std::move(session),
                                      4 /* queue length */);
            }
        }

        if (numMatchedDevices == 0) {
            std::vector<std::string> deviceNames;
            std::string deviceNamesStr;
            for (const auto & d: devices) {
                deviceNames.push_back(d->name());
                if (!deviceNamesStr.empty())
                    deviceNamesStr += ", ";
                deviceNamesStr += d->name();
            }

            throw HttpReturnException
                (400, "Device name regex '" + jsonEncodeStr(deviceFilter)
                 + "' didn't match any of the system devices "
                 + deviceNamesStr + ".  The graph cannot be executed");
        }
        //std::this_thread::sleep_for(std::chrono::seconds(120));
    }

    /// The actual graph to run
    std::unique_ptr<tensorflow::GraphDef> graph;

    /// Timestamp at which the model was created
    Date modelTs = Date::notADate();

    struct DeviceSession {
        DeviceSession(std::string device,
                      std::unique_ptr<tensorflow::Session> session,
                      int queueLength)
            : device(std::move(device)),
              session(std::move(session)),
              queueLength(queueLength),
              numQueued(0)
        {
        }

        std::string device;
        std::unique_ptr<tensorflow::Session> session;
        int queueLength;
        int numQueued;
    };

    std::map<std::string, tensorflow::Tensor> constants;  // const once constructed

    mutable std::vector<DeviceSession> sessions;  // mutable for numQueued

    struct Job {
        std::function<void ()> done;
    };

    mutable std::mutex queueLock;
    mutable std::condition_variable queueCond;
    

    Any getStatus() const
    {
        return Any();
    }

    Any getDetails() const
    {
        Json::Value result;
        result["graph"] = jsonEncode(ML::split(SummarizeGraphDef(*graph), '\n'));
        return result;
    }

    /** Used to bind the output of a Tensorflow graph into an SQL
        expression that extracts from it.
    */
    struct GraphExtractScope: public ReadThroughBindingScope {
        GraphExtractScope(SqlBindingScope & outerScope,
                          const tensorflow::GraphDef & graph)
            : ReadThroughBindingScope(outerScope)
        {
            // Go through all of the layers of the graph and index
            // them by node name
            for (const auto & node: graph.node()) {
                graphNodes[node.name()] = &node;
            }            
        }
        
        // Derives from inner row scope, so we can pass directly through
        struct RowScope: public ReadThroughBindingScope::RowScope {
            RowScope(const SqlRowScope & outerScope,
                     const std::vector<tensorflow::Tensor> & graphOutput,
                     Date ts)
                : ReadThroughBindingScope::RowScope(outerScope),
                  graphOutput(graphOutput),
                  ts(ts)
            {
            }

            /// The output of the graph
            const std::vector<tensorflow::Tensor> & graphOutput;

            /// The timestamp to apply to outputs
            Date ts;
        };

        std::map<Utf8String, const tensorflow::NodeDef *> graphNodes;
        std::map<ColumnPath, int> nodesRead;  // index into outputLayers
        std::vector<Utf8String> outputLayers;

        ColumnGetter doGetColumn(const Utf8String & tableName,
                                 const ColumnPath & columnName)
        {
            if (!tableName.empty())
                return ReadThroughBindingScope
                    ::doGetColumn(tableName, columnName);

            auto it = graphNodes.find(columnName.toSimpleName());
            if (it == graphNodes.end()) {
                // Not found in nodes; read through to the outside
                return ReadThroughBindingScope
                    ::doGetColumn(tableName, columnName);
            }
            
            // Record that this is a required output layer and what its
            // index is.  We use the index to look up the correct tensor
            // in the list of output tensors for the graph.
            int index = outputLayers.size();
            if (nodesRead.insert({columnName, index}).second)
                outputLayers.push_back(columnName.toSimpleName());
            else index = nodesRead[columnName];

            // Find the node, so we can figure out what kind of output
            // we have
            //const tensorflow::NodeDef * node = it->second;

            // TODO: tensor value info from the node
            auto info = std::make_shared<AnyValueInfo>();

            ColumnGetter result;
            result.exec = [=] (const SqlRowScope & scope_,
                               ExpressionValue & storage,
                               const VariableFilter & filter)
                -> const ExpressionValue &
                {
                    auto & scope = scope_.as<RowScope>();
                    storage = tensorToValue(scope.graphOutput.at(index),
                                            scope.ts);
                    return storage;
                };
            
            result.info = info;

            return result;
        }

        GetAllColumnsOutput
        doGetAllColumns(const Utf8String & tableName,
                        const ColumnFilter& keep)
        {
            if (!tableName.empty())
                return ReadThroughBindingScope
                    ::doGetAllColumns(tableName, keep);

            std::vector<KnownColumn> columns;
            std::vector<std::pair<PathElement, int> > indexes;

            for (const auto & n: graphNodes) {
                ColumnPath kept = keep(PathElement(n.first));
                if (kept.empty())
                    continue;
                if (kept.size() != 1)
                    throw HttpReturnException(500, "Not impl: compound names for input to TF graph");

                // Record that this is a required output layer and what its
                // index is.  We use the index to look up the correct tensor
                // in the list of output tensors for the graph.
                int index = outputLayers.size();
                if (nodesRead.insert({kept, index}).second)
                    outputLayers.push_back(n.first);
                else index = nodesRead[kept];

                // TODO: tensor value info from the node
                auto info = std::make_shared<AnyValueInfo>();

                columns.emplace_back(kept, info, COLUMN_IS_DENSE);
                indexes.emplace_back(kept[0], index);
            }

            GetAllColumnsOutput result;
            result.exec = [=] (const SqlRowScope & scope_, const VariableFilter & filter)
                -> ExpressionValue
                {
                    auto & scope = scope_.as<RowScope>();
                    StructValue result;
                    result.reserve(indexes.size());
                    for (const auto & i: indexes) {
                        result.emplace_back(i.first,
                                            tensorToValue(scope.graphOutput.at(i.second),
                                                          scope.ts));
                    }
                    return std::move(result);
                };
            
            result.info.reset(new RowValueInfo(std::move(columns),
                                               SCHEMA_CLOSED));

            return result;
        }
    };

    struct Applier: public FunctionApplier {
        Applier(const TensorflowGraphBase * owner,
                SqlBindingScope & outerScope,
                const std::shared_ptr<ExpressionValueInfo> & input,
                shared_ptr<spdlog::logger> logger)
            : FunctionApplier(owner),
              owner(owner),
              mldbScope(owner->server),
              functionScope(mldbScope, input),
              graphScope(outerScope, *owner->graph),
              logger(logger)
        {
            // 1.  Collect what is known for each of the input clauses.
            boundInputs = owner->inputs.bind(functionScope);

            // 2.  Collect whatever we need for the outputs.  We need to
            //     infer what is read from the graph to make it work.
            boundOutputs = owner->outputs.bind(graphScope);

            info.input = { input };
            info.output = ExpressionValueInfo::toRow(boundOutputs.info);
            
            // Check that all values on the passed input are compatible with the
            // required inputs.
            info.checkInputCompatibility(*input);
        }

        virtual ~Applier()
        {
        }

        const TensorflowGraphBase * owner;
        SqlExpressionMldbScope mldbScope;
        SqlExpressionExtractScope functionScope;
        GraphExtractScope graphScope;
        shared_ptr<spdlog::logger> logger;
        BoundSqlExpression boundInputs, boundOutputs;

        ExpressionValue apply(const ExpressionValue & inputData) const
        {
            ExpressionValue result;

            using namespace tensorflow;

            vector<Tensor> inputTensors;
            vector<string> inputLayers;

            auto rowScope = functionScope.getRowScope(inputData);

            ExpressionValue inStorage;
            const ExpressionValue & in
                = boundInputs(rowScope, inStorage, GET_LATEST);

            Date outputTs = owner->modelTs;

            for (const auto & inputColumn: boundInputs.info->getKnownColumns()) {
                std::string nodeName = inputColumn.columnName.toUtf8String().rawString();
                DEBUG_MSG(logger) << "input tensor node name " << nodeName;
                ExpressionValue field = in.getColumn(nodeName);
                
                outputTs.setMax(field.getEffectiveTimestamp());
                Tensor inputTensor = owner->getTensorFor(nodeName, field);
                
                inputTensors.emplace_back(std::move(inputTensor));
                inputLayers.emplace_back(std::move(nodeName));
            }

            vector<std::string> outputLayers;
            for (const auto & l: graphScope.outputLayers) {
                outputLayers.emplace_back(l.rawString());
            }

            vector<Tensor> outputs;

            auto doRun = [&] (int i)
                {
                    auto output = owner->call(inputTensors, inputLayers,
                                              outputLayers, i);

                    if (i == 0)
                        outputs = std::move(output);
                };


#if 0
            vector<std::thread> threads;
            for (int i = 0;  i < 100;  ++i) {
                threads.emplace_back([&,i] () { doRun(i); });
                //std::this_thread::sleep_for(std::chrono::milliseconds(10));
            }

            for (auto & t: threads)
                t.join();

#elif 0
            parallelMap(0, 100, doRun);
#else
            doRun(0);
#endif

            GraphExtractScope::RowScope outputRowScope(rowScope, outputs, outputTs);
            
            result = boundOutputs(outputRowScope, GET_LATEST);

            return result;
        }
    };

    virtual std::unique_ptr<FunctionApplier>
    bind(SqlBindingScope & outerScope,
         const std::vector<std::shared_ptr<ExpressionValueInfo> > & input)
        const override
    {
        if (input.size() != 1 || !input[0] || !input[0]->couldBeRow())
            throw HttpReturnException
                (400, "Tensorflow functions must be called with exactly "
                 "one row as input",
                 "inputs", input);
        std::unique_ptr<FunctionApplier> result
            (new Applier(this, outerScope, ExpressionValueInfo::toRow(input[0]), logger));
        return result;
    }

    static tensorflow::Tensor
    castToSizedAndTypedTensor(const ExpressionValue & val,
                              const tensorflow::TensorShape & shape,
                              tensorflow::DataType type)
    {
        const CellValue & input = val.getAtom();

        const unsigned char * data = input.blobData();
        const size_t len = input.blobLength();

        tensorflow::Tensor result(tensorflow::DT_STRING, {});
        
        auto str = result.flat<std::string>();
        str(0) = string(data, data + len);

        return result;
    }

    static tensorflow::Tensor
    castToTypedTensor(const ExpressionValue & val,
                      tensorflow::DataType type)
    {
        if (val.isAtom()) {
            const CellValue & atom = val.getAtom();
            switch (type) {
            case tensorflow::DT_FLOAT: {
                tensorflow::Tensor result(tensorflow::DT_FLOAT, {});
                result.flat<float>()(0) = atom.toDouble();
                return result;
            }
            case tensorflow::DT_DOUBLE: {
                tensorflow::Tensor result(tensorflow::DT_DOUBLE, {});
                result.flat<double>()(0) = atom.toDouble();
                return result;
            }
            case tensorflow::DT_INT32: {
                tensorflow::Tensor result(tensorflow::DT_INT32, {});
                result.flat<int32_t>()(0) = atom.toInt();
                return result;
            }
            case tensorflow::DT_UINT8: {
                tensorflow::Tensor result(tensorflow::DT_UINT8, {});
                result.flat<uint8_t>()(0) = atom.toInt();
                return result;
            }
            case tensorflow::DT_INT16: {
                tensorflow::Tensor result(tensorflow::DT_INT16, {});
                result.flat<int16_t>()(0) = atom.toInt();
                return result;
            }
            case tensorflow::DT_UINT16: {
                tensorflow::Tensor result(tensorflow::DT_UINT16, {});
                result.flat<uint16_t>()(0) = atom.toInt();
                return result;
            }
            case tensorflow::DT_INT8: {
                tensorflow::Tensor result(tensorflow::DT_INT8, {});
                result.flat<int8_t>()(0) = atom.toInt();
                return result;
            }
            case tensorflow::DT_STRING: {
                tensorflow::Tensor result(tensorflow::DT_STRING, {});
                if (atom.isBlob()) {
                    result.flat<std::string>()(0)
                        .append(atom.blobData(),
                                atom.blobData() + atom.blobLength());
                }
                else {
                    result.flat<std::string>()(0) = atom.toUtf8String().rawString();
                }
                return result;
            }
            case tensorflow::DT_INT64: {
                tensorflow::Tensor result(tensorflow::DT_INT64, {});
                result.flat<long long>()(0) = atom.toInt();
                return result;
            }
            default:
                break;
            }
        }
        else if (val.isEmbedding()) {
            tensorflow::TensorShape shape;
            size_t len = 1;
            for (int64_t s: val.getEmbeddingShape()) {
                shape.AddDim(s);
                len *= s;
            }

            switch (type) {
                case tensorflow::DT_FLOAT: {
                    tensorflow::Tensor result(type, shape);
                    val.convertEmbedding(result.flat<float>().data(),
                                         len, ST_FLOAT32);
                    return result;
                }
                case tensorflow::DT_DOUBLE: {
                    tensorflow::Tensor result(type, shape);
                    val.convertEmbedding(result.flat<double>().data(),
                                         len, ST_FLOAT64);
                    return result;
                }
                case tensorflow::DT_INT32: {
                    tensorflow::Tensor result(type, shape);
                    val.convertEmbedding(result.flat<int32_t>().data(),
                                         len, ST_INT32);
                    return result;
                }
                case tensorflow::DT_UINT8: {
                    tensorflow::Tensor result(type, shape);
                    val.convertEmbedding(result.flat<uint8_t>().data(),
                                         len, ST_UINT8);
                    return result;
                }
                case tensorflow::DT_INT16: {
                    tensorflow::Tensor result(type, shape);
                    val.convertEmbedding(result.flat<int16_t>().data(),
                                         len, ST_INT16);
                    return result;
                }
                case tensorflow::DT_UINT16: {
                    tensorflow::Tensor result(type, shape);
                        val.convertEmbedding(result.flat<uint16_t>().data(),
                                          len, ST_UINT16);
                    return result;
                }
                case tensorflow::DT_INT8: {
                    tensorflow::Tensor result(type, shape);
                    val.convertEmbedding(result.flat<int8_t>().data(),
                                         len, ST_INT8);
                    return result;
                }
                case tensorflow::DT_STRING: {
                    tensorflow::Tensor result(type, shape);
                    val.convertEmbedding(result.flat<std::string>().data(),
                                         len, ST_STRING);
                    return result;
                }
                case tensorflow::DT_INT64: {
                    tensorflow::Tensor result(type, shape);
                    val.convertEmbedding(result.flat<long long>().data(),
                                         len, ST_INT64);
                    return result;
                }
            default:
                break;
            }
        }
        else if (val.isArray()) {
            return castToTypedTensor(val.coerceToEmbedding(), type);
        }

        //cerr << "val = " << jsonEncode(val) << endl;
        //cerr << "type = " << type << endl;
        //cerr << "value type " << val.getTypeAsString() << endl;
        throw HttpReturnException(500, "Unable to cast value to typed tensor");
    }
    
    static tensorflow::Tensor
    castToTensor(const ExpressionValue & val)
    {
        if (val.isAtom()) {
            tensorflow::TensorShape shape { 1 };

            const CellValue & atom = val.getAtom();

            switch (atom.cellType()) {
            case CellValue::EMPTY: {
                tensorflow::Tensor result(tensorflow::DT_INT64, {1});
                return result;
            }
            case CellValue::INTEGER: {
                tensorflow::Tensor result(tensorflow::DT_INT64, {1});
                result.flat<long long>()(0) = atom.toInt();
                return result;
            }
            case CellValue::FLOAT: {
                tensorflow::Tensor result(tensorflow::DT_DOUBLE, {1});
                result.flat<double>()(0) = atom.toDouble();
                return result;
            }
            case CellValue::ASCII_STRING: {
                tensorflow::Tensor result(tensorflow::DT_STRING, {1});
                result.flat<std::string>()(0) = atom.toString();
                return result;
            }
            case CellValue::UTF8_STRING: {
                tensorflow::Tensor result(tensorflow::DT_STRING, {1});
                result.flat<std::string>()(0) = atom.toUtf8String().rawString();
                return result;
            }
            case CellValue::TIMESTAMP: {
                tensorflow::Tensor result(tensorflow::DT_DOUBLE, {1});
                result.flat<double>()(0) = atom.toTimestamp().secondsSinceEpoch();
                return result;
            }
            case CellValue::TIMEINTERVAL: {
                throw HttpReturnException(400, "Can't pass TIMEINTERVAL to Tensorflow");
            }
            case CellValue::BLOB: {
                tensorflow::Tensor result(tensorflow::DT_UINT8, {atom.blobLength()});
                std::copy(atom.blobData(),
                          atom.blobData() + atom.blobLength(),
                          result.flat<uint8_t>().data());
                return result;
            }
            case CellValue::PATH: {
                Path path = atom.coerceToPath();
                tensorflow::Tensor result(tensorflow::DT_STRING, {(int)path.size()});
                auto flat = result.flat<std::string>();
                for (size_t i = 0;  i < path.size();  ++i) {
                    flat(i) = path[i].toUtf8String().rawString();
                }
                return result;
            }
            case CellValue::NUM_CELL_TYPES:
                break;
            }
        }
        else if (val.isEmbedding()) {
            auto storageType = val.getEmbeddingType();
            if (storageType == ST_ATOM) {
                storageType = ST_FLOAT64; //TODO not sure its possible to get consistent type then
            }
            return castToTypedTensor(val, storageToDatatype(storageType));
        }

        //cerr << "trying to cast " << jsonEncode(val) << " to tensor" << endl;
        throw HttpReturnException(500, "Unable to cast value of type " + val.getTypeAsString() + " to tensor");
    }
    
    tensorflow::Tensor
    getTensorFor(const std::string & layer,
                 const ExpressionValue & val) const
    {
        TRACE_MSG(logger) << "input value for tensor is '" << jsonEncode(val) << "'";
        for (const auto & node: graph->node()) {
            if (node.name() == layer) {
                auto it = node.attr().find("value");
                if (it != node.attr().end()) {
                    // It has a value.  Attempt to match the datatype and
                    // the entire size.
                    DEBUG_MSG(logger) << "found value for tensor node '" << node.name()
                                      << "' of type '" << it->second.tensor().dtype()
                                      << "' and shape '" << it->second.tensor().tensor_shape().DebugString() << "'";
                    return castToSizedAndTypedTensor
                        (val,
                         tensorflow::TensorShape(it->second.tensor().tensor_shape()),
                         it->second.tensor().dtype());
                }
                it = node.attr().find("dtype");
                if (it != node.attr().end()) {
                    auto it2 = node.attr().find("shape");
                    if (it2 != node.attr().end()) {
                        DEBUG_MSG(logger) << "found type '" << it->second.tensor().dtype()
                                          << "' and shape '" << it2->second.tensor().tensor_shape().DebugString()
                                          << "' for tensor node '" << node.name() << "'";
                        // Match datatype and shape
                        return castToSizedAndTypedTensor
                            (val,
                             tensorflow::TensorShape(it2->second.shape()),
                             it->second.type());
                    }

                    // It has a datatype, but no value (and hence size).
                    // Attempt to match the data type only.

                    //cerr << "casting type-no-size: " << layer << endl;
                    //cerr << "value: " << jsonEncode(val) << endl;

                    return castToTypedTensor(val, it->second.type());
                }

                //cerr << "getting tensor for layer " << layer
                //     << " with description " << node.DebugString()
                //     << endl;

                // The layer has neither a size nor a datatype.  Convert it
                // in the natural way, and hope for the best.
                return castToTensor(val);

#if 0
                cerr << "found node " << layer << endl;
                cerr << "op = " << node.op() << endl;
                cerr << "dtype = " << node.attr().find("dtype")->second.DebugString() << endl;
                for (const const auto & attr: node.attr()) {
                    cerr << "attr " << attr.first << " " << attr.second.DebugString()
                         << endl;
                }
                for (const const auto & input: node.input()) {
                    cerr << "input " << input << endl;
                }
#endif
            }
        }

        throw HttpReturnException(400, "Unable to find layer '" + layer
                                  + "' to get tensor for");
    }

    template<typename T>
    static ExpressionValue tensorToValueT(const tensorflow::Tensor & tensor,
                                          Date ts,
                                          T * = nullptr)
    {
        if (tensor.dims() == 0) {
            // Scalar
            auto flattened = tensor.flat<T>();
            CellValue cell(flattened(0));
            return ExpressionValue(std::move(cell), ts);
        }
        
        auto flattened = tensor.flat<T>();
        size_t n = flattened.size();
        DimsVector shape;
        for (unsigned i = 0;  i < tensor.dims();  ++i) {
            shape.emplace_back(tensor.dim_size(i));
        }
        vector<CellValue> cells(n);
        for (size_t i = 0;  i < n;  ++i) {
            cells[i] = flattened(i);
        }
        return ExpressionValue(std::move(cells), ts, shape);
    }

    static ExpressionValue tensorToValueT(const tensorflow::Tensor & tensor,
                                          Date ts,
                                          std::string * = nullptr)
    {
        if (tensor.dims() == 0) {
            // Scalar
            auto flattened = tensor.flat<std::string>();
            try {
                MLDB_TRACE_EXCEPTIONS(false);
                CellValue cell(flattened(0));
                return ExpressionValue(std::move(cell), ts);
            } catch (const std::exception & exc) {
                auto cell = CellValue::blob(flattened(0).data(),
                                            flattened(0).size());
                return ExpressionValue(std::move(cell), ts);
            }
        }
        
        auto flattened = tensor.flat<std::string>();
        size_t n = flattened.size();
        DimsVector shape;
        for (unsigned i = 0;  i < tensor.dims();  ++i) {
            shape.emplace_back(tensor.dim_size(i));
        }
        vector<CellValue> cells(n);
        for (size_t i = 0;  i < n;  ++i) {
            try {
                MLDB_TRACE_EXCEPTIONS(false);
                cells[i] = flattened(i);
            } catch (const std::exception & exc) {
                cells[i] = CellValue::blob(flattened(i).data(),
                                           flattened(i).size());
            }
        }
        return ExpressionValue(std::move(cells), ts, shape);
    }

    static StorageType dataTypeToStorage(tensorflow::DataType dt)
    {
        using namespace tensorflow;

        switch (dt) {
        case DT_FLOAT:
            return ST_FLOAT32;
        case DT_DOUBLE:
            return ST_FLOAT64;
        case DT_INT32:
        case DT_QINT32:
            return ST_INT32;
        case DT_UINT8:
        case DT_QUINT8:
            return ST_UINT8;
        case DT_INT8:
        case DT_QINT8:
            return ST_INT8;
        case DT_INT16:
        case DT_QINT16:
            return ST_INT16;
        case DT_UINT16:
        case DT_QUINT16:
            return ST_UINT16;
        case DT_STRING:
            return ST_BLOB;
        case DT_BOOL:
            return ST_INT32;
        default:
            throw HttpReturnException(400, "Can't return tensor of this type from TensorFlow",
                                      "type", dt);
        }
    }

    static ExpressionValue tensorToValue(const tensorflow::Tensor & tensor,
                                         Date ts)
    {
        using namespace tensorflow;

        //cerr << "converting " << tensor.DebugString() << " to ExpressionValue"
        //     << endl;

        switch (tensor.dtype()) {
        case DT_FLOAT:
            return tensorToValueT<float>(tensor, ts);
        case DT_DOUBLE:
            return tensorToValueT<double>(tensor, ts);
        case DT_INT32:
            return tensorToValueT<int32_t>(tensor, ts);
        case DT_UINT8:
            return tensorToValueT<uint8_t>(tensor, ts);
        case DT_INT16:
            return tensorToValueT<int16_t>(tensor, ts);
        case DT_UINT16:
            return tensorToValueT<uint16_t>(tensor, ts);
        case DT_INT8:
            return tensorToValueT<int8_t>(tensor, ts);
        case DT_STRING:
            return tensorToValueT(tensor, ts, (std::string*)nullptr);
        case DT_INT64:
            return tensorToValueT<long long>(tensor, ts);
        case DT_BOOL:
            return tensorToValueT<bool>(tensor, ts);
            /*
              DT_QINT8 = 11;     // Quantized int8
              DT_QUINT8 = 12;    // Quantized uint8
              DT_QINT32 = 13;    // Quantized int32
              DT_BFLOAT16 = 14;  // Float32 truncated to 16 bits.  Only for cast ops.
              DT_QINT16 = 15;    // Quantized int16
              DT_QUINT16 = 16;   // Quantized uint16
            */
        default:
            throw HttpReturnException(400, "Can't return tensor of this type from TensorFlow"/*,
                                                                                               "type", tensor.dtype()*/);
        }
    }

    static tensorflow::DataType storageToDatatype(StorageType type)
    {
        using namespace tensorflow;

        switch (type) {
            case ST_FLOAT32:
                return DT_FLOAT;
            case ST_FLOAT64:
                return DT_DOUBLE;
            case ST_INT32:
                return DT_INT32;
            case ST_UINT8:
                return DT_UINT8;
            case ST_INT8:
                return DT_INT8;
            case ST_INT16:
                return DT_INT16;
            case ST_UINT16:
                return DT_UINT16;
            case ST_BLOB:
                return DT_STRING;
            case ST_TIMESTAMP:
            case ST_TIMEINTERVAL:
                return DT_DOUBLE;
            default:

            throw HttpReturnException(400, "Can't return value of this type to TensorFlow",
                                      "type", type);
        }
    }


    std::pair<std::shared_ptr<tensorflow::Session>, std::string>
    getSession() const
    {
        std::unique_lock<std::mutex> guard(queueLock);
        while (true) {
            cerr << "spin on session " << &guard << endl;
            int bestSession = -1;
            double bestSessionScore = INFINITY;
            for (unsigned i = 0;  i < sessions.size();  ++i) {
                if (sessions[i].numQueued < sessions[i].queueLength) {
                    double score = 1.0 * sessions[i].numQueued / sessions[i].queueLength;
                    if (score < bestSessionScore || bestSession == -1) {
                        bestSession = i;
                        bestSessionScore = score;
                    }
                }
            }

            cerr << "bestSession = " << bestSession << endl;

            if (bestSession != -1) {
                ++sessions[bestSession].numQueued;
                auto onDel = [bestSession, this] (tensorflow::Session *)
                    {
                        cerr << "before release session" << endl;
                        std::unique_lock<std::mutex> guard(this->queueLock);
                        cerr << "after release session" << endl;
                        --this->sessions[bestSession].numQueued;
                        this->queueCond.notify_one();
                    };

                return { std::shared_ptr<tensorflow::Session>(sessions[bestSession].session.get(), onDel), sessions[bestSession].device };
            }


            queueCond.wait(guard);
        }
    }

    std::vector<tensorflow::Tensor>
    call(const std::vector<tensorflow::Tensor> & inputs,
         const std::vector<string> & inputLayers,
         const std::vector<string> & outputLayers,
         int n) const
    {
        using namespace tensorflow;

        ExcAssertEqual(inputs.size(), inputLayers.size());

        // Note: passing constants in makes startup faster but causes
        // problems accessing them, so it's not done.
        vector<pair<string, Tensor> > inputTensors; // = constants;
        for (unsigned i = 0;  i < inputs.size();  ++i) {
            bool foundInput = false;
            // For each of them, replace the given entry with the
            // input version.
            for (unsigned j = 0;  j < inputTensors.size();  ++j) {
                if (inputTensors[j].first == inputLayers[i]) {
                    inputTensors[j].second = inputs[i];
                    foundInput = true;
                    break;
                }
            }

            if (!foundInput) {
                DEBUG_MSG(logger) << "found input tensor with name: '"
                                  << inputLayers[i] << "'";
                inputTensors.emplace_back(inputLayers[i], inputs[i]);
            }
        }

        Date before = Date::now();

        std::vector<Tensor> outputs;

        auto session = getSession();

        Date gotSession = Date::now();

        tensorflow::StepStats stats;
        //Status run_status = session.first
        //    ->RunWithStats(inputTensors, outputLayers,
        //                   {}, &outputs, &stats);

        DEBUG_MSG(logger) << "running tensor for outputs " << jsonEncode(outputLayers);

        Status run_status = session.first
            ->Run(inputTensors, outputLayers,
                  {}, &outputs);
        
        if (!run_status.ok()) {
            DEBUG_MSG(logger) << "unable to run tensor of shape " <<
                inputTensors[0].second.shape().DebugString();
            throw HttpReturnException(400, "Unable to run model: "
                                      + run_status.error_message());
        }
        
        Date after = Date::now();
        cerr << "latency on " << session.second << " was "
             << after.secondsSince(gotSession) * 1000 << "ms"
             << " plus " << gotSession.secondsSince(before) * 1000
             << " ms to get session" << endl;

#if 0
        uint64_t earliest = -1;

        struct NodeStats {
            uint64_t sched;     ///< Time until it was scheduled
            uint64_t wait;      ///< Time from scheduled until started running
            uint64_t pre;       ///< Time from start running to op running
            uint64_t run;       ///< Time spent running in op
            uint64_t post;      ///< Time post-operation
            double mem;         ///< Memory used
        };

        std::map<std::pair<std::string, std::string>, NodeStats> nodeStats;

        for (const auto & d: stats.dev_stats()) {
            for (const auto & n: d.node_stats()) {
                earliest = std::min<uint64_t>(earliest, n.scheduled_micros());
            }
        }

        for (const auto & d: stats.dev_stats()) {
            for (const auto & n: d.node_stats()) {
                NodeStats stats;
                stats.sched = n.scheduled_micros() - earliest;
                stats.wait  = n.all_start_micros() - n.scheduled_micros();
                stats.pre = n.op_start_rel_micros();
                stats.run = n.op_end_rel_micros();
                stats.post = n.all_end_rel_micros() - n.op_end_rel_micros();

                nodeStats.insert({{d.device(), n.node_name()}, stats});
            }
        }

        std::vector<std::pair<std::pair<std::string, std::string>, NodeStats> > sortedStats(nodeStats.begin(), nodeStats.end());

        auto cmp = [] (const std::pair<std::pair<std::string, std::string>, NodeStats> & s1,
                        const std::pair<std::pair<std::string, std::string>, NodeStats> & s2) -> bool
            {
                return s1.second.sched < s2.second.sched;
            };
        
        std::sort(sortedStats.begin(), sortedStats.end(),
                  cmp);
        
        static std::mutex mtx;
        std::unique_lock<std::mutex> guard(mtx);

        cerr << "-------------------------------------" << endl;

        int i = 0;
        for (const auto & st: sortedStats) {
            if (i++ % 50 == 0)
                cerr << "device    \tkernel                                            \tsched\twait\tpre\trun\tpost" << endl;
                    cerr << MLDB::format("%-10s", string(st.first.first, st.first.first.length() - 5).c_str())
                 << "\t" << MLDB::format("%-50s", st.first.second.c_str()) << "\t"
                 << st.second.sched << "\t" << st.second.wait << "\t"
                 << st.second.pre << "\t" << st.second.run << "\t"
                 << st.second.post << endl;
        }
#endif
        
        //cerr << "stats = " << stats.DebugString() << endl;

        //cerr << "outputs " << outputs.size() << " tensors" << endl;

        return outputs;
    }

    virtual ExpressionValue
    apply(const FunctionApplier & applier,
          const ExpressionValue & context) const override
    {
        //cerr << "applying tensor flow function, context: " << jsonEncode(context) << endl;

        return static_cast<const Applier &>(applier)
            .apply(context);
    }

    virtual FunctionInfo
    getFunctionInfo() const override
    {
        // Create a function binding context that can infer the
        // required inputs
        SqlExpressionMldbScope mldbScope(server);

        SqlExpressionExtractScope functionScope(mldbScope);

        // 1.  Collect what is known for each of the input clauses.
        auto boundInputs = inputs.bind(functionScope);

        GraphExtractScope graphScope(functionScope, *graph);

        auto boundOutputs = outputs.bind(graphScope);

        functionScope.inferInput();
        
        FunctionInfo result;
        result.input = { functionScope.inputInfo };
        result.output = ExpressionValueInfo::toRow(boundOutputs.info);
        
        return result;
    }

};


/*****************************************************************************/
/* TENSORFLOW OP                                                             */
/*****************************************************************************/

struct TensorflowOpConfig {
    Utf8String op;
    std::map<Utf8String, tensorflow::AttrValue> attr;
    SelectExpression inputs;
    SelectExpression outputs;
    Regex devices = ".*";
    tensorflow::DataType inputType = tensorflow::DT_INVALID;
};

DECLARE_STRUCTURE_DESCRIPTION(TensorflowOpConfig);

DEFINE_STRUCTURE_DESCRIPTION(TensorflowOpConfig);

TensorflowOpConfigDescription::
TensorflowOpConfigDescription()
{
    addField("op", &TensorflowOpConfig::op,
             "Name of operation to apply");
    addField("attr", &TensorflowOpConfig::attr,
             "Attributes of op");
    addField("inputs", &TensorflowOpConfig::inputs,
             "Input values for graph");
    addField("inputType", &TensorflowOpConfig::inputType,
             "Input type for graph nodes.  Default ('DT_INVALID') means "
             "infer it from the types on the operation.",
             tensorflow::DT_INVALID);
    addAuto("devices", &TensorflowOpConfig::devices,
            "Regular expression that matches the devices on which the "
            "operation is allowed to run.  For example, `.*` means all devices "
            "(CPU and GPU), `/cpu:.*` means CPU only, `/gpu:.*` means GPU "
            "only, `/gpu:[01]` means on the first two GPUs.");
}

struct TensorflowOp: public TensorflowGraphBase {

    TensorflowOpConfig functionConfig;

    TensorflowOp(MldbServer * owner,
                 PolyConfig config,
                 const std::function<bool (const Json::Value &)> & onProgress)
        : TensorflowGraphBase(owner, config)
    {
        functionConfig = config.params.convert<TensorflowOpConfig>();   
        tensorflow::Status status;
        status = tensorflow::OpRegistry::Global()->LookUpOpDef(functionConfig.op.rawString(),
                                                      &op);

        if (!op) {
            throw HttpReturnException(400, "Unable to obtain TensorFlow operator '"
                                      + functionConfig.op + "': "
                                      + status.error_message());
        }

        // Create a graph containing just the op
        std::unique_ptr<tensorflow::GraphDef>
            graph(new tensorflow::GraphDef());

        for (const auto & input: op->input_arg()) {
            tensorflow::NodeDef * inode = graph->add_node();
            //cerr << "input arg " << input.name() << endl;
            inode->set_name(input.name());
            inode->set_op("Placeholder");
            inode->set_device("/cpu:0");
            // TODO: generalize
            if (functionConfig.inputType != tensorflow::DT_INVALID)
                (*inode->mutable_attr())["dtype"].set_type(functionConfig.inputType);
        }

        tensorflow::NodeDef * node = graph->add_node();
        node->set_name("op");
        node->set_op(functionConfig.op.rawString());
        node->set_device("/cpu:0");

        auto * nattr = node->mutable_attr();

        for (const auto & attr: functionConfig.attr) {
            (*nattr)[attr.first.rawString()] = attr.second;
        }

        // Create a node for each input
        for (const auto & input: op->input_arg()) {
            node->add_input(input.name());
        }
        
        this->init(std::move(graph), functionConfig.inputs, functionConfig.outputs,
                   functionConfig.devices);

        //cerr << SummarizeGraphDef(*this->graph);
    }

#if 0
    Any getStatus() const
    {
        return Any();
    }

    virtual Any getDetails() const
    {
        return Any();
    }
#endif

    virtual RestRequestMatchResult
    handleRequest(RestConnection & connection,
                  const RestRequest & request,
                  RestRequestParsingContext & context) const
    {
        return MR_NO;
    }

    const tensorflow::OpDef * op;
};

static RegisterFunctionType<TensorflowOp, TensorflowOpConfig>
regTensorflowOp(tensorflowPackage(),
                "tensorflow.op",
                "Run a single Tensorflow operation",
                "TensorflowGraph.md.html",
                nullptr,
                { MldbEntity::INTERNAL_ENTITY });


/*****************************************************************************/
/* TENSORFLOW GRAPH                                                          */
/*****************************************************************************/

struct TensorflowGraphConfig {
    Url modelFileUrl;
    SelectExpression inputs;
    SelectExpression outputs;
    Regex devices = ".*";
};


DECLARE_STRUCTURE_DESCRIPTION(TensorflowGraphConfig);

DEFINE_STRUCTURE_DESCRIPTION(TensorflowGraphConfig);

TensorflowGraphConfigDescription::
TensorflowGraphConfigDescription()
{
    addField("modelFileUrl", &TensorflowGraphConfig::modelFileUrl,
             "Model file to load graph from.  This is probably a .pb "
             "file (protobuf file).");
    addField("inputs", &TensorflowGraphConfig::inputs,
             "Inputs to the graph, including names");
    addField("outputs", &TensorflowGraphConfig::outputs,
             "Outputs of the graph that are returned as the result of "
             "the function");
    addAuto("devices", &TensorflowGraphConfig::devices,
            "Regular expression that matches the devices on which the "
            "graph is allowed to run.  For example, `.*` means all devices "
            "(CPU and GPU), `/cpu:.*` means CPU only, `/gpu:.*` means GPU "
            "only, `/gpu:[01]` means on the first two GPUs.");
}

struct TensorflowGraph: public TensorflowGraphBase {

    TensorflowGraphConfig functionConfig;

    TensorflowGraph(MldbServer * owner,
                    PolyConfig config,
                    const std::function<bool (const Json::Value &)> & onProgress)
        : TensorflowGraphBase(owner, config)
    {
        functionConfig = config.params.convert<TensorflowGraphConfig>();   

        using namespace tensorflow;

        std::string graphContents;

        filter_istream stream(functionConfig.modelFileUrl);
        modelTs = stream.info().lastModified;
        
        google::protobuf::io::IstreamInputStream pstream(&stream);

        ::tensorflow::protobuf::io::CodedInputStream cstream(&pstream);

        // Allow large objects to be loaded, bypassing security
        // restrictions in Protobuf to avoid DOS attacks.
        cstream.SetTotalBytesLimit(1024LL << 20, 512LL << 20);
        
        graph.reset(new tensorflow::GraphDef());
        if (!graph->ParseFromCodedStream(&cstream)) {
            throw HttpReturnException
                (500, "Couldn't load tensorflow graph model: parse error");
        }

        this->init(std::move(graph), functionConfig.inputs, functionConfig.outputs,
                   functionConfig.devices);

        //cerr << SummarizeGraphDef(*this->graph);
    }

};

static RegisterFunctionType<TensorflowGraph, TensorflowGraphConfig>
regTensorflowGraph(tensorflowPackage(),
                   "tensorflow.graph",
                   "Graph parameters for a trained TensorFlow model",
                   "TensorflowGraph.md.html");


/*****************************************************************************/
/* PLUGIN                                                                    */
/*****************************************************************************/

// Plugin entry point.  This is called by MLDB once the plugin is loaded.
// We initialize the TensorFlow system.

struct TensorflowPlugin: public Plugin {
    TensorflowPlugin(MldbServer * server)
        : Plugin(server)
    {
        
        using namespace MLDB;

        int argc = 0;
        char ** argv = new char * [2];
        argv[0] = strdup("mldb");
        argv[1] = nullptr;

        //cerr << "Initializing TensorFlow" << endl;
        tensorflow::port::InitMain(argv[0], &argc, &argv);

        using namespace tensorflow;

        // Register a builtin function for each of the Tensorflow ops
        bool include_internal = true;
        OpList ops;
        OpRegistry::Global()->Export(include_internal, &ops);
    
        for (unsigned i = 0;  i < ops.op_size();  ++i) {
            const tensorflow::OpDef & op = ops.op(i);
            RegisteredOp entry;

            Status status = OpRegistry::Global()->LookUpOpDef(op.name(), &entry.op);
            ExcAssert(status.ok());

            entry.builtinFunctionHandle
                = registerFunction("tf_" + op.name(), wrapOp(op.name()));
            registeredOps[op.name()] = entry;
        }

        initRoutes();

        tfOperationMacroHandle
            = registerMacro("tfOperation",
                            std::bind(&TensorflowPlugin::tfOperationMacro,
                                      this,
                                      std::placeholders::_1,
                                      std::placeholders::_2,
                                      std::placeholders::_3));

        tfOperationsMacroHandle
            = registerMacro("tfOperations",
                            std::bind(&TensorflowPlugin::tfOperationsMacro,
                                      this,
                                      std::placeholders::_1,
                                      std::placeholders::_2));
    }

    virtual Any getStatus() const
    {
        return Any();
    }

    template<typename T>
    void writeAttrListItem(MacroContext & context, const T & val)
    {
        using std::to_string;
        context.writeHtml("<code>");
        context.writeText(to_string(val));
        context.writeHtml("</code>");
    }

    void writeAttrListItem(MacroContext & context, const std::string & val)
    {
        context.writeHtml("<code>");
        context.writeText(val);
        context.writeHtml("</code>");
    }

    void writeAttrListItem(MacroContext & context, const tensorflow::DataType & val)
    {
        context.writeHtml("<code>");
        context.writeText(jsonEncode(val).asString());
        context.writeHtml("</code>");
    }

    template<typename T>
    void writeAttrListT(MacroContext & context,
                       const ::google::protobuf::RepeatedField<T>& val)
    {
        context.writeHtml("<ul>");

        for (const auto & v: val) {
            context.writeHtml("<li>");
            writeAttrListItem(context, v);
            context.writeHtml("</li>");
        }

        context.writeHtml("</ul>");
    }

    template<typename T>
    void writeAttrListType(MacroContext & context,
                           const ::google::protobuf::RepeatedField<T>& val)
    {
        context.writeHtml("<ul>");

        for (const auto & v: val) {
            context.writeHtml("<li>");
            writeAttrListItem(context, (tensorflow::DataType)v);
            context.writeHtml("</li>");
        }

        context.writeHtml("</ul>");
    }

    template<typename T>
    void writeAttrListT(MacroContext & context,
                       const ::google::protobuf::RepeatedPtrField<T>& val)
    {
        context.writeHtml("<ul>");

        for (const auto & v: val) {
            context.writeHtml("<li>");
            writeAttrListItem(context, v);
            context.writeHtml("</li>");
        }

        context.writeHtml("</ul>");
    }

    /** Documentation macro for TF operations */
    void tfOperationsMacro(MacroContext & context,
                           const std::string & macroName)
    {
        auto writeTocEntry = [&context](const Utf8String & op) {
            // anchor
            context.writeHtml("<li>");
            context.writeHtml("<a href=#");
            context.writeText(op);
            context.writeHtml(">");
            context.writeHtml("TensorFlow <code>");
            context.writeText(op);
            context.writeHtml("</code></a>");
            context.writeHtml("</li>");
        };
        
        // write the table of content
        context.writeHtml("<ul>");
        for (const auto & op : registeredOps)
            writeTocEntry(op.first);
        context.writeHtml("</ul>");

        for (const auto & op : registeredOps)
            tfOperationMacro(context, macroName, op.first);
    }

    void tfOperationMacro(MacroContext & context,
                          const std::string & macroName,
                          const Utf8String & op)
    {
        auto it = registeredOps.find(op);
        if (it == registeredOps.end()) {
            context.writeText("Couldn't find TensorFlow operation " + op);
            return;
        }

        auto writeAttrList = [&] (const tensorflow::AttrValue_ListValue & val)
            {
                if (val.s_size()) {
                    writeAttrListT(context, val.s());
                }
                if (val.i_size()) {
                    writeAttrListT(context, val.i());
                }
                if (val.f_size()) {
                    writeAttrListT(context, val.f());
                }
                if (val.b_size()) {
                    writeAttrListT(context, val.b());
                }
                if (val.type_size()) {
                    // Protobuf makes the DataType fields ints, so we need to coerce
                    writeAttrListType(context, val.type());
                }
                if (val.shape_size()) {
                    //writeAttrListT(context, val.shape());
                }
                if (val.tensor_size()) {
                    //writeAttrListT(context, val.tensor());
                }
            };

        try {
            auto * opDef = it->second.op;
             // anchor
            context.writeHtml("<a name=\"");
            context.writeText(op);
            context.writeHtml("\"></a>");
            // heading
            context.writeHtml("<h2>TensorFlow <code>");
            context.writeText(op);
            context.writeHtml("</code> Operation</h2>");
            context.writeHtml("<h3>Description</h3>");
            context.writeMarkdown(opDef->description());
           

            if (opDef->attr_size()) {
                context.writeHtml("<h3>Attributes</h3>");
                context.writeHtml("<table><tr><th>Attribute</th><th>Type</th>"
                                  "<th>Default</th><th>Constraints</th>"
                                  "<th>Description</th></tr>");
                for (const auto & a: opDef->attr()) {
                    context.writeHtml("<tr><td>");
                    context.writeText(a.name());
                    context.writeHtml("</td><td>");
                    context.writeText(a.type());
                    context.writeHtml("</td><td>");
                    context.writeText(a.default_value().DebugString());
                    context.writeHtml("</td><td>");
                    if (a.has_allowed_values()) {
                        context.writeHtml("<b>One of: </b>");
                        writeAttrList(a.allowed_values().list());
                    }
                    if (a.has_minimum()) {
                        context.writeHtml("<b>Minimum: </b>");
                        context.writeText(to_string(a.minimum()));
                    }
                    context.writeHtml("</td><td>");
                    context.writeMarkdown(a.description());
                    context.writeHtml("</td></tr>");
                    
                }
                context.writeHtml("</table>");
            }

            typedef ::google::protobuf::RepeatedPtrField< ::tensorflow::OpDef_ArgDef >
                ArgDefList;

            auto writeArgTable = [&] (const ArgDefList & list,
                                      const std::string & title)
            {
                context.writeHtml("<h3>");
                context.writeText(title);
                context.writeHtml("</h3>");
                context.writeHtml("<table><tr><th>Name</th><th>Type</th>"
                                  "<th>Constraints</th><th>Description</th></tr>");
                for (const auto & a: list) {
                    context.writeHtml("<tr><td>");
                    // name
                    context.writeHtml("<code>");
                    context.writeText(a.name());
                    context.writeHtml("</code>");
                    context.writeHtml("</td><td>");
                    // type
                    if (a.type() != tensorflow::DT_INVALID) {
                        context.writeText(jsonEncode(a.type()).asString());
                    }
                    if (!a.type_attr().empty()) {
                        context.writeHtml("<b>attr: </b>");
                        context.writeHtml("<code>");
                        context.writeText(a.type_attr());
                        context.writeHtml("</code>");
                    }
                    if (!a.type_list_attr().empty()) {
                        context.writeHtml("<b>type list: </b>");
                        context.writeHtml("<code>");
                        context.writeText(a.type_list_attr());
                        context.writeHtml("</code>");
                    }

                    context.writeHtml("</td><td>");

                    // Constraints
                    if (!a.number_attr().empty()) {
                        context.writeHtml("<b>number: </b>");
                        context.writeHtml("<code>");
                        context.writeMarkdown(a.number_attr());
                        context.writeHtml("</code>");
                    }
                    context.writeHtml("</td><td>");
                    context.writeMarkdown(a.description());
                    context.writeHtml("</td></tr>");
                    
                }
                context.writeHtml("</table>");
            };

            if (opDef->input_arg_size()) {
                writeArgTable(opDef->input_arg(), "Input Arguments");
            }

            if (opDef->output_arg_size()) {
                writeArgTable(opDef->output_arg(), "Output Arguments");
            }
        } catch (const std::exception & exc) {
            context.writeText("error running TensorFlow operation macro for '" + op + "' :" + exc.what());
        }
    }

    /// Used to wrap an operation into an MLDB function
    ExternalFunction wrapOp(const std::string & op)
    {
        // Arguments:
        // 0.  Object value containing input pin names
        // 1.  Constant object value containing attributes
        return [=] (const Utf8String &,
                    const std::vector<BoundSqlExpression> & args,
                    SqlBindingScope & context)
            -> BoundFunction
            {
                
                const tensorflow::OpDef * opDef;
                tensorflow::Status status
                    = tensorflow::OpRegistry::Global()->LookUpOpDef(op, &opDef);

                if (args.size() < 1 || args.size() > 2)
                    throw HttpReturnException
                        (400, "Tensorflow builtin functions take one or two arguments");

                TensorflowOpConfig config;
                config.op = op;
                if (args.size() > 1) {
                    config.attr = jsonDecode<decltype(config.attr)>
                        (args[1].constantValue().extractJson());
                }

                for (const auto & input: opDef->input_arg()) {
                    config.inputs.clauses.emplace_back
                        (SqlRowExpression::parse(input.name()));
                }

                config.outputs.clauses.emplace_back(SqlRowExpression::parse("op"));

                //cerr << "opdef is " << opDef->DebugString() << endl;
                
                std::shared_ptr<RowValueInfo> inputInfo;

                bool singleInput = false;
                bool atomInput = args[0].info->isScalar();
                PathElement singleInputName;

                
                // Check if there is only one input; we synthesize the full
                // set of inputs if so
                if (args[0].info->isScalar() || args[0].info->isEmbedding()) {
                    if (opDef->input_arg_size() == 1) {
                        singleInput = true;
                        singleInputName = opDef->input_arg(0).name();

                        std::vector<KnownColumn> columns;
                        columns.emplace_back(singleInputName,
                                             args[0].info,
                                             COLUMN_IS_DENSE,
                                             0 /* index */);
                        inputInfo.reset(new RowValueInfo(columns, SCHEMA_CLOSED));
                    }
                    else {
                        throw HttpReturnException
                            (400, "Attempt to pass scalar to Tensorflow function '"
                             + op + "' which takes more than one input"); 
                    }
                }
                else if (args[0].info->isRow()) {
                    inputInfo = ExpressionValueInfo::toRow(args[0].info);
                }
                else {
                    //we dont know the type of input
                    inputInfo.reset(new UnknownRowValueInfo());
                }

                for (size_t i = 0;  i < opDef->input_arg_size();  ++i) {
                    const auto & inputArg = opDef->input_arg(i);

                    if (inputArg.type()) {
                        config.inputType = inputArg.type();
                    }
                    else if (!inputArg.type_attr().empty()) {
                        std::string attrName = inputArg.type_attr();
                        auto it = config.attr.find(attrName);
                        if (it == config.attr.end()) {
                            // We didn't find the attribute
                            // Set it to the default value
                            for (auto & attr: opDef->attr()) {
                                if (attr.name() == attrName) {
                                    // found it
                                    config.inputType = attr.default_value().type();
                                }
                            }
                            // Can't do it
                        } else {
                            config.inputType = it->second.type();
                        }
                    }
                }

                // Now bind the input of the function, so that we know what
                // kind of tensor will be in the arguments

                PolyConfig pconfig;
                pconfig.params = config;
                auto fn = std::make_shared<TensorflowOp>
                    (server, pconfig, nullptr /* progress */);

                std::shared_ptr<spdlog::logger> logger = MLDB::getMldbLog<TensorflowOp>();
                fn->logger = std::move(logger);

                std::shared_ptr<FunctionApplier> applier
                    (fn->bind(context, {inputInfo})
                     .release());
                
                BoundFunction result;
                result.exec = [=] (const std::vector<ExpressionValue> & args,
                                   const SqlRowScope & context)
                    -> ExpressionValue
                    {
                        if (singleInput) {
                            StructValue val;
                            val.emplace_back(singleInputName, args[0]);
                            if (atomInput)
                                return fn->apply(*applier, val).getColumn("op");
                            else
                                return fn->apply(*applier, val).getColumn("op");
                        }
                        else {
                            return fn->apply(*applier, args.at(0)).getColumn("op");
                        }
                    };

                result.resultInfo = applier->info.output;
                return result;
            };
    }

    RestRequestRouter router;

    // List all of the known operations
    std::vector<Utf8String> listOps() const
    {
        std::vector<Utf8String> result;
        for (const auto & o: registeredOps) {
            result.push_back(o.first);
        }
        return result;
    }

    Json::Value getOpInfo(const Utf8String & op) const
    {
        auto it = registeredOps.find(op);
        if (it == registeredOps.end()) {
            throw HttpReturnException(404, "Operation '" + op
                                      + "' not found in this version of TensorFlow");
        }

        Json::Value result;
        result["info"] = protoToJson(*it->second.op);
        return result;
    }

    // Initialize the routes
    void initRoutes()
    {
        auto & ops = router.addSubRouter("/ops", "Tensorflow operations");
        
        addRouteSyncJsonReturn(ops, "", { "GET" },
                               "Return a list of supported operations",
                               "List of operation names",
                               &TensorflowPlugin::listOps,
                               this);
        auto & op
            = ops.addSubRouter(Rx("/([0-9a-zA-Z]*)", "/<OpName>"),
                               "Information about an individual op");

        // This is the 7th parameter:
        // /v1=0 /plugins=1 /tensorflow=2 tensorflow=3 /routes=4 /ops=5 /<OpName>=6 <OpName>=7
        RequestParam<Utf8String> opName(7, "<OpName>",
                                        "Operation to operate on");
        
        addRouteSyncJsonReturn(op, "", { "GET" },
                               "Return information about a Tensorflow operation",
                               "JSON structure with information",
                               &TensorflowPlugin::getOpInfo,
                               this,
                               opName);
    }

    virtual RestRequestMatchResult
    handleRequest(RestConnection & connection,
                  const RestRequest & request,
                  RestRequestParsingContext & context) const
    {
        return router.processRequest(connection, request, context);
    }

    struct RegisteredOp {
        std::shared_ptr<void> builtinFunctionHandle;
        const tensorflow::OpDef * op;
    };
    

    /// Contains the handle to builtin functions registered by the plugin to cover
    /// all of the Tensorflow ops
    std::map<Utf8String, RegisteredOp> registeredOps;

    /// Handle to the documentation macros we register
    std::shared_ptr<void> tfOperationMacroHandle; // document one operation
    std::shared_ptr<void> tfOperationsMacroHandle; // document all operations
};

// tf_node function

BoundFunction
tf_extract_constant (const Utf8String &functionName,
                     const std::vector<BoundSqlExpression> & args,
                     SqlBindingScope & context)
{
    // Return an escaped string from a path
    checkArgsSize(args.size(), 2);

    MldbServer * server = context.getMldbServer();

    auto exec = [=] (const std::vector<ExpressionValue> & args,
                     const SqlRowScope & context)
        -> ExpressionValue
        {
            checkArgsSize(args.size(), 2);
            Utf8String graphName = args[0].toUtf8String();
            PolyConfig pconfig;
            pconfig.id = graphName;
            auto graph = std::dynamic_pointer_cast<TensorflowGraphBase>
                (obtainFunction(server, pconfig));

            if (!graph) {
                throw HttpReturnException
                    (404, "No TensorFlow graph with name found");
            }

            Path path = args[1].coerceToPath();

            std::string key;
            for (PathElement p: path) {
                if (!key.empty())
                    key += '/';
                key += p.toUtf8String().extractAscii();
            }

            auto it = graph->constants.find(key);

            if (it == graph->constants.end()) {
                std::vector<Utf8String> constantExamples;
                size_t n = 0;
                for (auto & c: graph->constants) {
                    constantExamples.emplace_back(c.first);
                    if (n++ > 100)
                        break;
                }

                throw HttpReturnException
                    (500, "Constant " + path.toUtf8String()
                     + " not found (path is " + key + ")",
                     "examples", constantExamples);
            }

            auto result = TensorflowGraphBase::tensorToValue
                (it->second, graph->modelTs);

            return result;
        };

    return {
        exec,
        std::make_shared<EmbeddingValueInfo>()
    };
}

static RegisterFunction registerTfExtractConstant("tf_extract_constant", tf_extract_constant);

} // namespace MLDB


MLDB::Plugin *
mldbPluginEnterV100(MLDB::MldbServer * server)
{
    return new MLDB::TensorflowPlugin(server);
}

