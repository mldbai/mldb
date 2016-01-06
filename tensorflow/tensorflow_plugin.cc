/** tensorflow_plugin.cc
    Jeremy Barnes, 24 November 2015
    Copyright (c) 2015 Datacratic Inc.  All rights reserved.

*/

#include "mldb/core/mldb_entity.h"
#include "mldb/core/function.h"
#include "mldb/core/plugin.h"
#include "mldb/types/structure_description.h"
#include "mldb/types/vector_description.h"
#include "mldb/types/url.h"
#include "mldb/types/any_impl.h"
#include "mldb/arch/timers.h"
#include "mldb/jml/utils/worker_task.h"
#include "mldb/jml/utils/string_functions.h"
#include "mldb/vfs/filter_streams.h"
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
#include "tensorflow/core/public/tensor.h"
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

// Plugin entry point.  This is called by MLDB once the plugin is loaded.
// We initialize the TensorFlow system.

Datacratic::MLDB::Plugin *
mldbPluginEnterV100(Datacratic::MLDB::MldbServer * server)
{
    using namespace Datacratic;
    using namespace Datacratic::MLDB;

    int argc = 0;
    char ** argv = new char * [2];
    argv[0] = strdup("myprogram");
    argv[1] = nullptr;

    cerr << "Initializing TensorFlow" << endl;
    tensorflow::port::InitMain(argv[0], &argc, &argv);

    using namespace tensorflow;

#if 0    
    bool include_internal = true;
    OpList ops;
    OpRegistry::Global()->Export(include_internal, &ops);
    
    cerr << "there are " << ops.op_size() << " ops registered" << endl;
    for (unsigned i = 0;  i < ops.op_size();  ++i) {
        cerr << ops.op(i).name() << " " << ops.op(i).summary() << endl;
    }
#endif

#if 0
    string graph_file_name = "inception/tensorflow_inception_graph.pb";

    tensorflow::GraphDef graph_def;
    Status load_graph_status =
        ReadBinaryProto(tensorflow::Env::Default(), graph_file_name, &graph_def);
    if (!load_graph_status.ok()) {
        throw HttpReturnException(500, "Couldn't load Inception model");
    }

    std::unique_ptr<Session> session(tensorflow::NewSession(tensorflow::SessionOptions()));
    Status session_create_status = session->Create(graph_def);
    
    if (!session_create_status.ok()) {
        throw HttpReturnException(500, "Couldn't initialize Inception session");
    }

    Tensor resizedImage;
    
    {

        std::string file_name = "ext/tensorflow/tensorflow/examples/label_image/data/grace_hopper.jpg";

        int input_width = 299;
        int input_height = 299;
        float input_mean = 128;
        float input_std = 128;

        tensorflow::GraphDefBuilder b;
        string input_name = "file_reader";
        string output_name = "normalized";
        tensorflow::Node* file_reader =
            tensorflow::ops::ReadFile(tensorflow::ops::Const(StringPiece(file_name), b.opts()),
                                      b.opts().WithName(input_name));

        // Now try to figure out what kind of file it is and decode it.
        const int wanted_channels = 3;
        tensorflow::Node* image_reader;
        if (tensorflow::StringPiece(file_name).ends_with(".png")) {
            image_reader = tensorflow::ops::DecodePng(
                                                      file_reader,
                                                      b.opts().WithAttr("channels", wanted_channels).WithName("png_reader"));
        } else {
            // Assume if it's not a PNG then it must be a JPEG.
            image_reader = tensorflow::ops::DecodeJpeg(
                                                       file_reader,
                                                       b.opts().WithAttr("channels", wanted_channels).WithName("jpeg_reader"));
        }
        // Now cast the image data to float so we can do normal math on it.
        tensorflow::Node* float_caster = tensorflow::ops::Cast(
                                                               image_reader, tensorflow::DT_FLOAT, b.opts().WithName("float_caster"));
        // The convention for image ops in TensorFlow is that all images are expected
        // to be in batches, so that they're four-dimensional arrays with indices of
        // [batch, height, width, channel]. Because we only have a single image, we
        // have to add a batch dimension of 1 to the start with ExpandDims().
        tensorflow::Node* dims_expander = tensorflow::ops::ExpandDims(
                                                                      float_caster, tensorflow::ops::Const(0, b.opts()), b.opts());
        // Bilinearly resize the image to fit the required dimensions.
        tensorflow::Node* resized = tensorflow::ops::ResizeBilinear(
                                                                    dims_expander, tensorflow::ops::Const({input_height, input_width},
                                                                                                          b.opts().WithName("size")),
                                                                    b.opts());
        // Subtract the mean and divide by the scale.
        tensorflow::ops::Div(
                             tensorflow::ops::Sub(
                                                  resized, tensorflow::ops::Const({input_mean}, b.opts()), b.opts()),
                             tensorflow::ops::Const({input_std}, b.opts()),
                             b.opts().WithName(output_name));

        // This runs the GraphDef network definition that we've just constructed, and
        // returns the results in the output tensor.
        tensorflow::GraphDef graph;

        auto graphRes = b.ToGraphDef(&graph);
        if (!graphRes.ok())
            throw HttpReturnException(400, "Unable to construct the graph: "
                                      + graphRes.error_message());


        cerr << "Graph is" << endl;
        cerr << SummarizeGraphDef(graph);

        std::string type_url = "/" + graph.GetDescriptor()->full_name();
        std::string bin;
        graph.SerializeToString(&bin);
        std::string jstring;

        std::string url_prefix;

        google::protobuf::util::TypeResolver* resolver
            = google::protobuf::util::NewTypeResolverForDescriptorPool
            (url_prefix, google::protobuf::DescriptorPool::generated_pool());
        
        cerr << "resolver = " << resolver << endl;

        protobuf::util::JsonOptions options;
        options.add_whitespace = true;

        auto res = protobuf::util::BinaryToJsonString(resolver,
                                                      type_url,
                                                      bin,
                                                      &jstring,
                                                      options);
        
        cerr << "res.error_message() = " << res.error_message() << endl;

        cerr << "def is " << jstring << endl;

        tensorflow::SessionOptions options;
        options.config.mutable_device_count->at("GPU") = 0;
        options.config.mutable_device_count->at("gpu") = 0;
        options.config.mutable_device_count->at("CPU") = 1;
        options.config.add_device_filters("/cpu:0");

        std::unique_ptr<Session> session(tensorflow::NewSession(options));

        auto createRes = session->Create(graph);
        if (!createRes.ok())
            throw HttpReturnException(400, "Unable to create graph: " + createRes.error_message());

        std::vector<Tensor> out_tensors;
        tensorflow::StepStats stats;
        auto runRes = session->RunWithStats({}, {output_name}, {}, &out_tensors, &stats);
        if (!runRes.ok())
            throw HttpReturnException(400, "Unable to run output tensors: " + runRes.error_message());

        cerr << "returned " << out_tensors.size() << " tensors" << endl;

        cerr << stats->DebugPrint();

        resizedImage = std::move(out_tensors.at(0));
    }

    string input_layer = "Mul";
    string output_layer = "softmax";

    // Actually run the image through the model.
    Tensor output;

    ML::Timer timer;

    auto onRun = [&] (int n)
        {
            std::vector<Tensor> outputs;
            Status run_status = session->Run({{input_layer, resizedImage}},
                {output_layer}, {}, &outputs);

            if (!run_status.ok()) {
                throw HttpReturnException(400, "Unable to run model: "
                                          + run_status.error_message());
            }

            cerr << "outputs " << outputs.size() << " tensors" << endl;

            if (n == 0)
                output = std::move(outputs.at(0));
        };

    ML::run_in_parallel(0, 20, onRun);

    cerr << "elapsed " << timer.elapsed() << endl;

    auto scores = output.flat<float>();

    vector<pair<float, int> > sorted;
    for (unsigned i = 0;  i < scores.size();  ++i)
        sorted.emplace_back(scores(i), i);

    std::sort(sorted.begin(), sorted.end());
    std::reverse(sorted.begin(), sorted.end());

    for (unsigned i = 0;  i < 5 && i < sorted.size();  ++i) {
        cerr << "category " << sorted[i].second << " score " << sorted[i].first
             << endl;
    }

#endif

#if 0
    cerr << "output tensor has " << output.shape().dims() << " dims" << endl;
    for (unsigned i = 0;  i < output.shape().dims();  ++i) {
        cerr << "dim " << i << " has value " << output.shape().dim_size(i)
             << endl;
    }
#endif
    
    return nullptr;
}


namespace Datacratic {
namespace MLDB {

const Package & tensorflowPackage()
{
    static const Package result("tensorflow");
    return result;
}

#if 0
/*****************************************************************************/
/* TENSORFLOW KERNEL                                                         */
/*****************************************************************************/

struct TensorflowKernelConfig {
};

DECLARE_STRUCTURE_DESCRIPTION(TensorflowKernelConfig);

DEFINE_STRUCTURE_DESCRIPTION(TensorflowKernelConfig);

TensorflowKernelConfigDescription::
TensorflowKernelConfigDescription()
{
}

struct TensorflowKernel: public Function {

    TensorflowKernelConfig functionConfig;

    TensorflowKernel(MldbServer * owner,
                     PolyConfig config,
                     const std::function<bool (const Json::Value &)> & onProgress)
        : Function(owner)
    {
        functionConfig = config.params.convert<TensorflowKernelConfig>();   
    }

    Any getStatus() const
    {
        return Any();
    }

    FunctionOutput
    apply(const FunctionApplier & applier,
          const FunctionContext & context) const
    {
        FunctionOutput result;

        Utf8String output("output");
        result.set("output", ExpressionValue("hello", Date::notADate()));
    
        return result;
    }

    FunctionInfo
    getFunctionInfo() const
    {
        FunctionInfo result;

        result.input.addAtomValue("text");
        result.output.addAtomValue("output");
    
        return result;
    }

};
#endif


/*****************************************************************************/
/* TENSORFLOW GRAPH                                                          */
/*****************************************************************************/

struct TensorflowGraphConfig {
    Url modelFileUrl;
    SelectExpression inputs;
    SelectExpression outputs;
};


DECLARE_STRUCTURE_DESCRIPTION(TensorflowGraphConfig);

DEFINE_STRUCTURE_DESCRIPTION(TensorflowGraphConfig);

TensorflowGraphConfigDescription::
TensorflowGraphConfigDescription()
{
    addField("modelFileUrl", &TensorflowGraphConfig::modelFileUrl,
             "Model file to load graph from.  This is probable a .pb "
             "file (protobuf file).");
    addField("inputs", &TensorflowGraphConfig::inputs,
             "Inputs to the graph, including names");
    addField("outputs", &TensorflowGraphConfig::outputs,
             "Outputs of the graph that are returned as the result of "
             "the function");
}

struct TensorflowGraph: public Function {

    TensorflowGraphConfig functionConfig;

    TensorflowGraph(MldbServer * owner,
                    PolyConfig config,
                    const std::function<bool (const Json::Value &)> & onProgress)
        : Function(owner)
    {
        functionConfig = config.params.convert<TensorflowGraphConfig>();   

        using namespace tensorflow;

        std::string graphContents;

        ML::filter_istream stream(functionConfig.modelFileUrl.toString());
        
        google::protobuf::io::IstreamInputStream pstream(&stream);

        ::tensorflow::protobuf::io::CodedInputStream cstream(&pstream);

        // Allow large objects to be loaded
        cstream.SetTotalBytesLimit(1024LL << 20, 512LL << 20);
        
        graph.reset(new tensorflow::GraphDef());
        if (!graph->ParseFromCodedStream(&cstream)) {
            throw HttpReturnException(500, "Couldn't load tensorflow graph model: parse error");
        }

        // Those without a device set can be bound to multiple
        // places.
        std::set<std::string> nodesWithoutDevices;

        // These nodes have constants which we shouldn't need to spend
        // time running one after the other
        std::map<std::string, Tensor> constants;

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
                if (attr.first == "use_cudnn_on_gpu") {
                    attr.second.set_b(false);
                }
            }
            //for (auto & input: node.input()) {
            //    cerr << "input " << input << endl;
            //}
        }

        cerr << "got " << constants.size() << " constants with "
             << constantTotalBytes << " total bytes" << endl;

        // Set them up as a list
        this->constants.insert(this->constants.end(),
                               constants.begin(),
                               constants.end());

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
            "ExpandDims", "ResizeBilinear", "Cast", "Sub", "Mul", "ExpandDims/dim" };

        for (auto & d: devices) {
            std::string deviceName = d->name();
            bool isCpuDevice = deviceName.find("/cpu:") != std::string::npos;

            // Set the device for all nodes where it's not hardcoded
            for (auto & node: *graph->mutable_node()) {
                if (nodesWithoutDevices.count(node.name())) {
                    bool isCpu = false;
                    for (auto & c: hardcodedCpu) {
                        if (node.name().find(c) == 0)
                            isCpu = true;
                    }

                    //auto it = hardcodedCpu.lower_bound(node.name());
                    //if (it != hardcodedCpu.end() && node.name().find(*it) == 0)
                    //isCpu = true;
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
                options.config.set_use_per_session_threads(true);
                options.config.set_inter_op_parallelism_threads(2);
            }
            else {
                // The GPU gets some CPU threads of its own so that
                // CPU operations can't block GPU operations.  This
                // is necessary to achieve maximum occupancy of the
                // GPU.
                options.config.set_allow_soft_placement(true);
                options.config.set_use_per_session_threads(true);
                options.config.set_inter_op_parallelism_threads(4);
            }

            // NOTE: eventually, this will work... but until it does, we
            // need to go through the above.  Currently Tensorflow just
            // ignores the device_filters fields.
            //options.config.add_device_filters(d->name());

            for (unsigned i = 0;  i < sessionsPerDevice;  ++i) {
                std::unique_ptr<tensorflow::Session> session;
                session.reset(tensorflow::NewSession(options));
                Status session_create_status = session->Create(*graph);
                
                if (!session_create_status.ok()) {
                    throw HttpReturnException(500, "Couldn't initialize tensorflow graph model: " + session_create_status.error_message());
                }
                
                sessions.emplace_back(d->name(), std::move(session), 2 /* queue length */);
            }
        }
    }

    std::unique_ptr<tensorflow::GraphDef> graph;

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

    std::vector<std::pair<std::string, tensorflow::Tensor> > constants;

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

#if 0
    virtual std::unique_ptr<FunctionApplier>
    bind(SqlBindingScope & outerContext,
         const FunctionValues & input) const
    {
        // 1.  Collect what is known for each of the input clauses.
        

        auto boundInputs = functionConfig.inputs.bind(outerContext);
    }
#endif

    std::pair<std::shared_ptr<tensorflow::Session>, std::string>
    getSession() const
    {
        std::unique_lock<std::mutex> guard(queueLock);
        while (true) {
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

            if (bestSession != -1) {
                ++sessions[bestSession].numQueued;
                auto onDel = [bestSession, this] (tensorflow::Session *)
                    {
                        std::unique_lock<std::mutex> guard(queueLock);
                        --sessions[bestSession].numQueued;
                        queueCond.notify_one();
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
         int n)
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
                }
                if (!foundInput)
                    inputTensors.emplace_back(inputLayers[i], inputs[i]);
            }
        }

        Date before = Date::now();

        std::vector<Tensor> outputs;

        auto session = getSession();

        tensorflow::StepStats stats;
        Status run_status = session.first
            ->RunWithStats(inputTensors, outputLayers,
                           {}, &outputs, &stats);
        
        if (!run_status.ok()) {
            throw HttpReturnException(400, "Unable to run model: "
                                      + run_status.error_message());
        }
        
        Date after = Date::now();
        cerr << "latency on " << session.second << " was "
             << after.secondsSince(before) * 1000 << "ms" << endl;

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

        for (auto & d: stats.dev_stats()) {
            for (auto & n: d.node_stats()) {
                earliest = std::min<uint64_t>(earliest, n.scheduled_micros());
            }
        }

        for (auto & d: stats.dev_stats()) {
            for (auto & n: d.node_stats()) {
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
        for (auto & st: sortedStats) {
            if (i++ % 50 == 0)
                cerr << "device    \tkernel                                            \tsched\twait\tpre\trun\tpost" << endl;
                    cerr << ML::format("%-10s", string(st.first.first, st.first.first.length() - 5).c_str())
                 << "\t" << ML::format("%-50s", st.first.second.c_str()) << "\t"
                 << st.second.sched << "\t" << st.second.wait << "\t"
                 << st.second.pre << "\t" << st.second.run << "\t"
                 << st.second.post << endl;
        }
#endif
        
        //cerr << "stats = " << stats.DebugString() << endl;

        //cerr << "outputs " << outputs.size() << " tensors" << endl;

        return std::move(outputs);
    }

    virtual FunctionOutput
    apply(const FunctionApplier & applier,
          const FunctionContext & context) const
    {
        FunctionOutput result;

        CellValue input = context.get<CellValue>("jpeg");
        
        const unsigned char * data = input.blobData();
        const size_t len = input.blobLength();

        using namespace tensorflow;

        Tensor inputTensor(DT_STRING, { });
        
        auto str = inputTensor.flat<std::string>();
        str(0) = string(data, data + len);

        string input_layer = "DecodeJpeg/contents";
        string output_layer = "softmax";

        vector<Tensor> outputs;

        auto doRun = [&] (int i)
            {
                auto output = call({ inputTensor }, { input_layer },
                                   { output_layer }, i);

                if (i == 0)
                    outputs = std::move(output);
            };


        vector<std::thread> threads;
        for (int i = 0;  i < 1000;  ++i) {
            threads.emplace_back([&,i] () { doRun(i); });
            //std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }

        for (auto & t: threads)
            t.join();


        //ML::run_in_parallel(0, 100, doRun);


        auto scores = outputs.at(0).flat<float>();
        vector<float> scores2(scores.data(), scores.data() + scores.size());

        Utf8String output("output");
        result.set("output", ExpressionValue(scores2, Date::notADate()));
    
        return result;
    }

    FunctionInfo
    getFunctionInfo() const
    {
        FunctionInfo result;

        result.input.addBlobValue("jpeg");
        result.output.addAtomValue("output");
    
        return result;
    }

};

static RegisterFunctionType<TensorflowGraph, TensorflowGraphConfig>
regTensorflowGraph(tensorflowPackage(),
                   "tensorflow.graph",
                   "Graph parameters for a trained TensorFlow model",
                   "TensorflowGraph.md");


} // namespace MLDB
} // namespace Datacratic
