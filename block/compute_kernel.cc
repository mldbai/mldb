#include "compute_kernel.h"
#include "mldb/base/parse_context.h"
#include "mldb/types/value_description.h"
#include "mldb/types/structure_description.h"
#include "mldb/types/vector_description.h"
#include "mldb/types/enum_description.h"
#include "mldb/types/meta_value_description.h"
#include "mldb/utils/command_expression.h"
#include <memory>

using namespace std;

namespace MLDB {

using namespace PluginCommand;

DEFINE_STRUCTURE_DESCRIPTION_INLINE(ComputeKernelDimension)
{
    addField("bound", &ComputeKernelDimension::bound, "");
}

DEFINE_STRUCTURE_DESCRIPTION_INLINE(ComputeKernelType)
{
    addField("str", &ComputeKernelType::str, "");
    addField("access", &ComputeKernelType::access, "");
    addField("baseType", &ComputeKernelType::baseType, "");
    addField("dims", &ComputeKernelType::dims, "");
}

DEFINE_ENUM_DESCRIPTION_INLINE(ComputeRuntimeId)
{
    addValue("NONE", ComputeRuntimeId::NONE, "No runtime selected");
    addValue("HOST", ComputeRuntimeId::HOST, "Runs on the host CPU");
    addValue("MULTI", ComputeRuntimeId::MULTI, "Runs across multiple runtimes");
    addValue("OPENCL", ComputeRuntimeId::OPENCL, "Runs on the OpenCL runtime");
    addValue("METAL", ComputeRuntimeId::METAL, "Runs on the Apple Metal runtime");
    addValue("CUDA", ComputeRuntimeId::CUDA, "Runs on the Nvidia CUDA runtime");
    addValue("ROCM", ComputeRuntimeId::ROCM, "Runs on the AMD ROCM runtime");
}

ComputeDevice ComputeDevice::defaultFor(ComputeRuntimeId id)
{
    auto runtime = ComputeRuntime::tryGetRuntimeForId(id);
    if (!runtime)
        throw MLDB::Exception("Unregistered runtime has no default device");
    return runtime->getDefaultDevice();
}

std::string ComputeDevice::info() const
{
    auto runtime = ComputeRuntime::tryGetRuntimeForId(this->runtime);
    if (!runtime)
        return "<<RUNTIME IS NOT REGISTERED>>";
    return runtime->printHumanReadableDeviceInfo(*this);
}

struct ComputeDeviceDescription 
    : public ValueDescriptionI<ComputeDevice, ValueKind::ATOM, ComputeDeviceDescription> {

    virtual void parseJsonTyped(ComputeDevice * val,
                                JsonParsingContext & context) const override
    {
        std::string s = context.expectStringAscii();
        throw MLDB::Exception("Unimplemented: ComputeDevice parseJsonTyped");
    }

    virtual void printJsonTyped(const ComputeDevice * val,
                                JsonPrintingContext & context) const override
    {
        std::string result;
        result += jsonEncode(val->runtime).asString();

        auto runtime = ComputeRuntime::tryGetRuntimeForId(val->runtime);
        if (!runtime) {
            if (val->runtimeInstance != 0 || val->deviceInstance != 0 || val->opaque1 != 0 || val->opaque2 != 0) {
                result += format(":%x", val->runtimeInstance);
            }
            if (val->deviceInstance != 0 || val->opaque1 != 0 || val->opaque2 != 0) {
                result += format(":%x", val->deviceInstance);
            }
            if (val->opaque1 != 0 || val->opaque2 != 0) {
                result += format(":%x", val->opaque1);
            }
            if (val->opaque2 != 0) {
                result += format(":%llx", (unsigned long long)val->opaque2);
            }
        }
        else {
            std::string rest = runtime->printRestOfDevice(*val);
            if (rest != "")
                result += ":" + rest;
        }

        context.writeString(result);
    }
};

DEFINE_VALUE_DESCRIPTION_NS(ComputeDevice, ComputeDeviceDescription);

std::ostream & operator << (std::ostream & stream, const ComputeDevice & device)
{
    return stream << jsonEncode(device).asString() << " (" << device.info() << ")";
}

namespace {

std::mutex basicTypeRegistryMutex;
std::map<std::string, std::shared_ptr<const ValueDescription>> basicTypeRegistry;

template<typename T>
struct RegisterBasicType {
    RegisterBasicType(const std::string & name)
    {
        std::unique_lock guard(basicTypeRegistryMutex);
        if (!basicTypeRegistry.emplace(name, getDefaultDescriptionSharedT<T>()).second)
            throw MLDB::Exception("Double registering basic type " + name);
    }
};

#define REGISTER_BASIC_TYPE(type, name) \
static const RegisterBasicType<type> doRegister##type(name);

REGISTER_BASIC_TYPE(uint64_t, "u64");
REGISTER_BASIC_TYPE(uint32_t, "u32");
REGISTER_BASIC_TYPE(uint16_t, "u16");
REGISTER_BASIC_TYPE(uint8_t,  "u8");
REGISTER_BASIC_TYPE(int64_t, "i64");
REGISTER_BASIC_TYPE(int32_t, "i32");
REGISTER_BASIC_TYPE(int16_t, "i16");
REGISTER_BASIC_TYPE(int8_t,  "i8");
REGISTER_BASIC_TYPE(float, "f32");
REGISTER_BASIC_TYPE(double, "f64");

} // file scope

// Parse the name of an atomic type
std::string expectTypeName(ParseContext & context)
{
    std::string result;

    if (!context)
        context.exception("Expected type name");

    if (*context == '\'') {
        ++context;
        result = context.expect_text("'");
        context.expect_literal('\'');
        return result;
    }

    ParseContext::Hold_Token token(context);

    while (context) {
        char c = *context;
        if (!isalnum(c) && c != '_' && c != ':' && c != '<' && c != '>')
            break;
        ++context;
    }

    result = token.captured();
    if (result.empty())
        context.exception("Expected type name");
    return result;
}

// Expect an actual type from the context
ComputeKernelType
expectType(ParseContext & context)
{
    std::string typeName = expectTypeName(context);

    {
        std::unique_lock guard(basicTypeRegistryMutex);
        auto it = basicTypeRegistry.find(typeName);
        if (it != basicTypeRegistry.end()) {
            return ComputeKernelType(typeName, it->second);
        }
    }

    ComputeKernelType result(typeName, ValueDescription::get(typeName));
    if (!result.baseType) {
        context.exception("Couldn't find type '" + typeName + "' in registry");
    }
    return result;
}

struct BoundsExpression {

};

ComputeKernelType
parseType(const std::string & type)
{
    ParseContext context(type, type.data(), type.data() + type.length());
    auto result = expectType(context);
    while (context.match_literal('[')) {
        // It's an array
        auto bound = CommandExpression::parseArgumentExpression(context);
        context.expect_literal(']', "expected closing array expression");
        result.dims.push_back({bound});
    }
    context.expect_eof();
    return result;
}

namespace {

std::mutex computeRuntimeRegistryMutex;

struct ComputeRuntimeEntry {
    std::string name;
    std::function<ComputeRuntime *()> create;
};

std::map<ComputeRuntimeId, ComputeRuntimeEntry> computeRuntimeRegistry;

} // file scope

void
ComputeRuntime::
registerRuntime(ComputeRuntimeId id, const std::string & name,
                std::function<ComputeRuntime *()> create)
{
    std::unique_lock guard(computeRuntimeRegistryMutex);
    auto [it, inserted] = computeRuntimeRegistry.insert({id, { name, create }});
    if (!inserted)
        throw MLDB::Exception("Double registration of compute runtime '" + name + "'");
}

std::shared_ptr<ComputeRuntime>
ComputeRuntime::
getRuntimeForDevice(ComputeDevice device)
{
    return getRuntimeForId(ComputeRuntimeId(device.runtime));
}

std::shared_ptr<ComputeRuntime>
ComputeRuntime::
tryGetRuntimeForId(ComputeRuntimeId id)
{
    static std::shared_ptr<ComputeRuntime> runtimes[256];
    std::shared_ptr<ComputeRuntime> runtime = std::atomic_load(runtimes + (uint8_t)id);
    if (runtime)
        return runtime;
    std::unique_lock guard(computeRuntimeRegistryMutex);
    auto it = computeRuntimeRegistry.find(id);
    if (it == computeRuntimeRegistry.end()) {
        return nullptr;
    }
    auto result = std::shared_ptr<ComputeRuntime>(it->second.create());
    std::atomic_store(runtimes + (uint8_t)id, result);
    return result;
}

std::shared_ptr<ComputeRuntime>
ComputeRuntime::
getRuntimeForId(ComputeRuntimeId id)
{
    auto result = tryGetRuntimeForId(id);
    if (!result)
        throw MLDB::Exception("Couldn't find compute runtime for runtime id " + jsonEncodeStr(id));
    return result;
}

std::shared_ptr<ComputeRuntime>
ComputeRuntime::
getDefault()
{
    return getRuntimeForDevice(ComputeDevice::host());
}

} // namespace MLDB
