#include "compute_kernel.h"
#include "mldb/base/parse_context.h"
#include "mldb/types/value_description.h"
#include "mldb/utils/command_expression.h"

using namespace std;

namespace MLDB {

using namespace PluginCommand;

namespace {

std::mutex kernelRegistryMutex;
struct KernelRegistryEntry {
    std::function<std::shared_ptr<ComputeKernel>(ComputeDevice)> generate;
};

std::map<std::string, KernelRegistryEntry> kernelRegistry;

} // file scope

namespace {


template<typename T>
struct RegisterIntegralType {
    RegisterIntegralType(const std::string & name)
    {
    }
};

#define REGISTER_INTEGRAL_TYPE(type, name) \
static const RegisterIntegralType<type> doRegister##type(name);

REGISTER_INTEGRAL_TYPE(uint64_t, "u64");
REGISTER_INTEGRAL_TYPE(uint32_t, "u32");
REGISTER_INTEGRAL_TYPE(uint16_t, "u16");
REGISTER_INTEGRAL_TYPE(uint8_t,  "u8");

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
        char c = *context++;
        if (isalnum(c) || c == '_' || c == ':')
            continue;
        break;
    }

    result = token.captured();
    if (result.empty())
        context.exception("Expected type name");
    return result;
}

// Expect an actual type from the context
std::shared_ptr<ComputeKernelType>
expectType(ParseContext & context)
{
    std::string typeName = expectTypeName(context);
    context.expect_eof();
    return std::make_shared<ComputeKernelType>();
}

struct BoundsExpression {

};

std::shared_ptr<const ComputeKernelType>
parseType(const std::string & type)
{
    ParseContext context(type, type.data(), type.data() + type.length());
    if (context.match_literal('[')) {
        // It's an array
        auto bounds = CommandExpression::parseExpression(context);
    }
    auto result = expectType(context);
    result->str = type;
    return result;
}

auto ComputeContext::getKernel(const std::string & kernelName, ComputeDevice device) -> std::shared_ptr<ComputeKernel>
{
    std::unique_lock guard(kernelRegistryMutex);
    auto it = kernelRegistry.find(kernelName);
    if (it == kernelRegistry.end()) {
        throw AnnotatedException(400, "Unable to find compute kernel '" + kernelName + "'",
                                    "kernelName", kernelName);
    }
    return it->second.generate(device);
}


void registerComputeKernel(const std::string & kernelName,
                           std::function<std::shared_ptr<ComputeKernel>(ComputeDevice device)> generator)
{
    kernelRegistry[kernelName].generate = generator;
}

} // namespace MLDB
