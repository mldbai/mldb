#include "compute_kernel.h"
#include "mldb/base/parse_context.h"
#include "mldb/types/value_description.h"
#include "mldb/types/structure_description.h"
#include "mldb/types/vector_description.h"
#include "mldb/types/enum_description.h"
#include "mldb/types/meta_value_description.h"
#include "mldb/types/map_description.h"
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

DEFINE_ENUM_DESCRIPTION_INLINE(MemoryRegionAccess)
{
    addValue("NONE", ACC_NONE, "No access");
    addValue("READ", ACC_NONE, "No access");
    addValue("WRITE", ACC_NONE, "No access");
    addValue("READ_WRITE", ACC_NONE, "No access");
}

DEFINE_STRUCTURE_DESCRIPTION_INLINE(ComputeKernelConstraint)
{
    addField("lhs", &ComputeKernelConstraint::lhs, "Left side of constraint expression");
    addField("op", &ComputeKernelConstraint::op, "Comparison operator for constraint");
    addField("rhs", &ComputeKernelConstraint::rhs, "Right side of constraint expression");
    addField("description", &ComputeKernelConstraint::description, "Description of where constraint comes from");
}

MemoryRegionAccess parseAccess(const std::string & accessStr)
{
    if (accessStr == "")
        return ACC_NONE;
    else if (accessStr == "r")
        return ACC_READ;
    else if (accessStr == "w")
        return ACC_WRITE;
    else if (accessStr == "rw")
        return ACC_READ_WRITE;
    else throw MLDB::Exception("Couldn't parse access specifier '" + accessStr + "': expected '', 'r', 'w' or 'rw'");
}
std::string printAccess(MemoryRegionAccess access)
{
    switch (access) {
    case ACC_NONE:          return "";
    case ACC_READ:          return "r";
    case ACC_WRITE:         return "w";
    case ACC_READ_WRITE:    return "rw";
    default:
        throw MLDB::Exception("unknown access specifier can't be printed");
    }
}

std::ostream & operator << (std::ostream & stream, MemoryRegionAccess access)
{
    return stream << printAccess(access);
}

DEFINE_ENUM_DESCRIPTION_INLINE(MemoryRegionInitialization)
{
    addValue("INIT_NONE", INIT_NONE, "No initialization; contents are indeterminate (with no sensitive data visible)");
    addValue("INIT_ZERO", INIT_ZERO_FILLED, "Fill with zeros");
    addValue("INIT_BLOCK", INIT_BLOCK_FILLED, "Fill with a memory block");
    addValue("INIT_KERNEL", INIT_KERNEL, "Fill by running a kernel");
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
std::map<std::string, std::string> reverseTypeRegistry;

template<typename T>
struct RegisterBasicType {
    RegisterBasicType(const std::string & name)
    {
        std::unique_lock guard(basicTypeRegistryMutex);
        if (!basicTypeRegistry.emplace(name, getDefaultDescriptionSharedT<T>()).second)
            throw MLDB::Exception("Double registering basic type " + name);
        reverseTypeRegistry[typeid(T).name()] = name;
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
            return ComputeKernelType(it->second, "");
        }
    }

    ComputeKernelType result(ValueDescription::get(typeName), ACC_NONE);
    if (!result.baseType) {
        context.exception("Couldn't find type '" + typeName + "' in registry");
    }
    return result;
}

ComputeKernelType
parseType(const std::string & access, const std::string & type)
{
    return parseType(access + " " + type);
}

ComputeKernelType
parseType(const std::string & accessAndType)
{
    ParseContext context(accessAndType, accessAndType.data(), accessAndType.data() + accessAndType.length());

    std::string access = context.expect_text(" ");
    context.expect_whitespace();

    auto result = expectType(context);
    while (context.match_literal('[')) {
        // It's an array
        auto bound = CommandExpression::parseArgumentExpression(context);
        context.expect_literal(']', "expected closing array expression");
        result.dims.push_back({bound});
    }
    context.expect_eof();
    result.access = parseAccess(access);
    return result;
}

namespace {
std::string
printBaseType(const ValueDescription & desc)
{
    std::string typeName = desc.typeName;

    if (desc.type) {
        {
            std::unique_lock guard(basicTypeRegistryMutex);
            auto it = reverseTypeRegistry.find(desc.type->name());
            if (it != reverseTypeRegistry.end())
                return typeName = it->second;
        }

        auto aliases = getValueDescriptionAliases(*desc.type);

        for (auto & a: aliases) {
            if (a.size() < typeName.size()) {
                typeName = std::move(a);
            }
        }
    }

    return typeName;
}
} // file scope

std::string
ComputeKernelType::
print() const
{
    if (!baseType)
        return "<<NULL>>";

    auto typeName = printBaseType(*baseType);

    auto result = printAccess(access) + " " + typeName;
    for (auto [bound]: dims) {
        result += "[" + (bound ? bound->surfaceForm : std::string()) + "]";
    }
    return result;
}

bool
ComputeKernelType::
isCompatibleWith(const ComputeKernelType & otherType, std::string * reason) const
{
    auto fail = [&] (std::string why) -> bool { if (reason) *reason = std::move(why);  return false; };

    if (!baseType || !otherType.baseType)
        return fail("base types are not completely specified: return " + print() + " vs passed " + otherType.print());

    if (*baseType->type != *otherType.baseType->type) {
        return fail("return type " + printBaseType(*baseType)
                    + " not same as passed type " + printBaseType(*otherType.baseType));
    }

    if (dims.size() != otherType.dims.size()) {
        return fail("different array dimensionality: return " + std::to_string(dims.size()) + " vs passed " + std::to_string(otherType.dims.size()));
    }

    if (access & ACC_WRITE) {
        if (!(otherType.access & ACC_WRITE)) {
            return fail("compatibility error: passing read-only to writable parameter");
        }
    }

    return true;
}


// AbstractArgumentHandler

bool
AbstractArgumentHandler::
canGetPrimitive() const
{
    return false;
}

std::span<const std::byte>
AbstractArgumentHandler::
getPrimitive(const std::string & opName, ComputeContext & context) const
{
    if (canGetPrimitive())
        throw MLDB::Exception("getPrimitive not overriden when canGetPrimitive is true");
    else
        throw MLDB::Exception("calling getPrimitive when canGetPrimitive is false");
}

bool
AbstractArgumentHandler::
canGetRange() const
{
    return canGetConstRange() && !isConst;
}

std::tuple<void *, size_t, std::shared_ptr<const void>>
AbstractArgumentHandler::
getRange(const std::string & opName, ComputeContext & context) const
{
    if (canGetRange())
        throw MLDB::Exception("getRange() not overriden when canGetRange() is true");
    else
        throw MLDB::Exception("calling getRange() when canGetRange() is false");
}

bool
AbstractArgumentHandler::
canGetConstRange() const
{
    return canGetRange();
}

std::tuple<const void *, size_t, std::shared_ptr<const void>>
AbstractArgumentHandler::
getConstRange(const std::string & opName, ComputeContext & context) const
{
    if (canGetConstRange())
        throw MLDB::Exception("getConstRange() not overriden when canGetConstRange() is true");
    else
        throw MLDB::Exception("calling getConstRange() when canGetConstRange() is false");
}

bool
AbstractArgumentHandler::
canGetHandle() const
{
    return false;
}

MemoryRegionHandle
AbstractArgumentHandler::
getHandle(const std::string & opName, ComputeContext & context) const
{
    if (canGetHandle())
        throw MLDB::Exception("getHandle() not overriden when canGetHandle() is true");
    else
        throw MLDB::Exception("calling getHandle() when canGetHandle() is false");
}

std::string
AbstractArgumentHandler::
info() const
{
    return type.print();
}

// ComputeQueue

ComputePromiseT<MemoryRegionHandle>
ComputeQueue::
enqueueFillArrayImpl(const std::string & opName,
                     MemoryRegionHandle regionIn, MemoryRegionInitialization init,
                     size_t startOffsetInBytes, ssize_t lengthInBytes,
                     const std::any & arg,
                     std::vector<std::shared_ptr<ComputeEvent>> prereqs)
{
    MemoryArrayHandleT<uint8_t> region{regionIn.handle};

    if (startOffsetInBytes > region.lengthInBytes()) {
        throw MLDB::Exception("enqueueFillArrayImpl: array start index out of bounds");
    }
    if (lengthInBytes == -1)
        lengthInBytes = region.lengthInBytes() - startOffsetInBytes;
    if (startOffsetInBytes + lengthInBytes > region.lengthInBytes()) {
        throw MLDB::Exception("enqueueFillArrayImpl: array end index out of bounds");
    }

    // Default implementation: launch a "fill" kernel
    switch (init) {
        case MemoryRegionInitialization::INIT_NONE:
            return { std::move(region), this->makeAlreadyResolvedEvent() };  // nothing to do 
        case MemoryRegionInitialization::INIT_ZERO_FILLED: {
            auto kernel = owner->getKernel("__zeroFillArray");
            auto bound = kernel->bind("region", region,
                                      "startOffsetInBytes", (uint64_t)startOffsetInBytes,
                                      "lengthInBytes", (uint64_t)lengthInBytes);
            return launch(opName, bound, {} /* grid */, {} /* prereqs */)
                ->then([result = std::move(regionIn)] () { return result; });
        }
        case MemoryRegionInitialization::INIT_BLOCK_FILLED: {
            auto kernel = owner->getKernel("__blockFillArray");
            auto block = std::any_cast<std::span<const std::byte>>(arg);
            auto blockRegion = owner->managePinnedHostRegionImpl(opName + " pin block", block, 1 /* align */,
                                                                 typeid(std::byte), true /* isConst */);
            MemoryArrayHandleT<uint8_t> blockHandle{std::move(blockRegion.move().handle)};
            auto bound = kernel->bind("region", region,
                                      "startOffsetInBytes", (uint64_t)startOffsetInBytes,
                                      "lengthInBytes", (uint64_t)lengthInBytes,
                                      "blockData", blockHandle,
                                      "blockLengthInBytes", (uint64_t)block.size());
            return launch(opName, bound, {} /* grid */, {} /* prereqs */)
                ->then([result = std::move(regionIn)] () { return result; });
        }
        case MemoryRegionInitialization::INIT_KERNEL: {
            auto bound = std::any_cast<BoundComputeKernel>(arg);
            return launch(opName, bound, {} /* grid */, {} /* prereqs */)
                ->then([result = std::move(regionIn)] () { return result; });
        }
    }
    throw MLDB::Exception("Unknown fillArray implementation");
}


// ComputeContext

MemoryRegionHandle
ComputeContext::
allocateSyncImpl(const std::string & regionName,
                    size_t length, size_t align,
                    const std::type_info & type, bool isConst,
                    MemoryRegionInitialization initialization,
                    std::any initWith)
{
    return allocateImpl(regionName, length, align, type, isConst, initialization,
                        std::move(initWith)).move();
}

MemoryRegionHandle
ComputeContext::
transferToDeviceSyncImpl(const std::string & opName,
                            FrozenMemoryRegion region,
                            const std::type_info & type, bool isConst)
{
    return transferToDeviceImpl(opName, std::move(region), type, isConst).move();
}

FrozenMemoryRegion
ComputeContext::
transferToHostSyncImpl(const std::string & opName,
                        MemoryRegionHandle handle)
{
    return transferToHostImpl(opName, std::move(handle)).move();
}

MutableMemoryRegion
ComputeContext::
transferToHostMutableSyncImpl(const std::string & opName,
                                MemoryRegionHandle handle)
{
    return transferToHostMutableImpl(opName, std::move(handle)).move();   
}

MemoryRegionHandle
ComputeContext::
managePinnedHostRegionSyncImpl(const std::string & opName,
                                std::span<const std::byte> region, size_t align,
                                const std::type_info & type, bool isConst)
{
    return managePinnedHostRegionImpl(opName, region, align, type, isConst).move();
}

void
ComputeContext::
fillDeviceRegionFromHostSyncImpl(const std::string & opName,
                                 MemoryRegionHandle deviceHandle,
                                 std::span<const std::byte> hostRegion,
                                 size_t deviceOffset)
{
    auto pinnedHostRegion = std::make_shared<std::span<const std::byte>>(hostRegion);
    fillDeviceRegionFromHostImpl(opName, deviceHandle, pinnedHostRegion, deviceOffset)->await();
}

void
ComputeContext::
copyBetweenDeviceRegionsSyncImpl(const std::string & opName,
                                 MemoryRegionHandle from, MemoryRegionHandle to,
                                 size_t fromOffset, size_t toOffset,
                                 size_t length)
{
    copyBetweenDeviceRegionsImpl(opName, from, to, fromOffset, toOffset, length)->await();
}


// ComputeRuntime

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
    using namespace std;
    cerr << "Registering runtime " << name << endl;
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

namespace {
    static std::shared_ptr<ComputeRuntime> runtimes[256];
} // file scope

std::shared_ptr<ComputeRuntime>
ComputeRuntime::
tryGetRuntimeForId(ComputeRuntimeId id)
{
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

std::vector<ComputeRuntimeId>
ComputeRuntime::
enumerateRegisteredRuntimes()
{
    std::vector<ComputeRuntimeId> result;
    std::unique_lock guard(computeRuntimeRegistryMutex);
    result.reserve(computeRuntimeRegistry.size());
    for (auto & [id,unused]: computeRuntimeRegistry)
        result.push_back(id);
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


// ComputeKernelConstraint

std::string
ComputeKernelConstraint::
print() const
{
    return lhs->surfaceForm + op + rhs->surfaceForm + " (" + description + ")";
}

namespace {

std::set<std::string> getVariables(const CommandExpression & expression)
{
    std::set<std::string> result;
    auto * var = dynamic_cast<const VariableExpression *>(&expression);
    if (var) {
        result = { var->variableName };
    }
    else {
        for (auto & clause: expression.childClauses()) {
            ExcAssert(clause);
            auto vars = getVariables(*clause);
            result.insert(vars.begin(), vars.end());
        }
    }
    return result;
}

} // file scope

bool
ComputeKernelConstraint::
attemptToSatisfy(CommandExpressionContext & context,
                 std::set<std::string> & unsatisfied) const
{
    if (op != "==")
        return false;  // equality constraints only for now

    bool canEvaluateLhs = true;
    for (auto var: getVariables(*lhs)) {
        if (!context.hasValue(var)) {
            canEvaluateLhs = false;
            unsatisfied.insert(var);
        }
    }

    Json::Value lhsVal;
    if (canEvaluateLhs) {
        lhsVal = lhs->apply(context);
    }

    bool canEvaluateRhs = true;
    for (auto var: getVariables(*rhs)) {
        if (!context.hasValue(var)) {
            canEvaluateRhs = false;
            unsatisfied.insert(var);
        }
    }

    Json::Value rhsVal;
    if (canEvaluateRhs) {
        rhsVal = rhs->apply(context);
    }
    
    if (!canEvaluateLhs && !canEvaluateRhs) {
        return false;
    }
    else if (canEvaluateLhs && canEvaluateRhs) {
        if (lhsVal != rhsVal) {
            cerr << "lhsVal.type() = " << lhsVal.type() << endl;
            cerr << "rhsVal.type() = " << rhsVal.type() << endl;
            cerr << "lhsVal = " << lhsVal << endl;
            cerr << "rhsVal = " << rhsVal << endl;
            cerr << "lhs = " << lhs->surfaceForm << endl;
            cerr << "rhs = " << rhs->surfaceForm << endl;
            cerr << "context = " << jsonEncode(context.values) << endl;
            throw MLDB::Exception("Couldn't meet constraint " + print()
                                  + ": (" + lhsVal.toStringNoNewLine()
                                  + " != " + rhsVal.toStringNoNewLine() + ")");
        }
    }

    const VariableExpression * lhsVar = dynamic_cast<const VariableExpression *>(lhs.get());
    const VariableExpression * rhsVar = dynamic_cast<const VariableExpression *>(rhs.get());

    if (lhsVar && canEvaluateRhs) {
        unsatisfied.erase(lhsVar->variableName);
        if (!context.hasValue(lhsVar->variableName)) {
            context.setValue(lhsVar->variableName, rhsVal);
            return true;
        }
    }
    else if (rhsVar && canEvaluateLhs) {
        unsatisfied.erase(rhsVar->variableName);
        if (!context.hasValue(rhsVar->variableName)) {
            context.setValue(rhsVar->variableName, lhsVal);
            return true;
        }
    }

    return false;
}

bool
ComputeKernelConstraint::
satisfied(CommandExpressionContext & context) const
{
    if (op != "==")
        return false;  // equality constraints only for now

    auto unknownLhs = lhs->unknowns(context);
    auto unknownRhs = rhs->unknowns(context);

    if (unknownLhs.empty() || unknownRhs.empty())
        return false;

    auto lhsVal = lhs->apply(context);
    auto rhsVal = rhs->apply(context);
    if (lhsVal != rhsVal)
        throw MLDB::Exception("constraint is unsatisfiable");
    return true;
}


// ComputeKernel

void
ComputeKernel::
addConstraint(const std::string lhs, const std::string & op, const std::string & rhs,
              const std::string & description)
{
    auto lhsParsed = CommandExpression::parseArgumentExpression(lhs);
    auto rhsParsed = CommandExpression::parseArgumentExpression(rhs);
    addConstraint(lhsParsed, op, rhsParsed, description);
}

void
ComputeKernel::
addConstraint(std::shared_ptr<const CommandExpression> lhs,
              const std::string & op, const std::string & rhs,
              const std::string & description)
{
    auto rhsParsed = CommandExpression::parseArgumentExpression(rhs);
    addConstraint(lhs, op, rhsParsed, description);
}

void
ComputeKernel::
addConstraint(std::shared_ptr<const CommandExpression> lhs,
              const std::string & op,
              std::shared_ptr<const CommandExpression> rhs,
              const std::string & description)
{
    constraints.push_back({lhs, op, rhs, description});
}


// BoundComputeKernel

void
BoundComputeKernel::
addConstraint(const std::string lhs, const std::string & op, const std::string & rhs,
              const std::string & description)
{
    auto lhsParsed = CommandExpression::parseArgumentExpression(lhs);
    auto rhsParsed = CommandExpression::parseArgumentExpression(rhs);
    addConstraint(lhsParsed, op, rhsParsed, description);
}

void
BoundComputeKernel::
addConstraint(std::shared_ptr<const CommandExpression> lhs,
              const std::string & op, const std::string & rhs,
              const std::string & description)
{
    auto rhsParsed = CommandExpression::parseArgumentExpression(rhs);
    addConstraint(lhs, op, rhsParsed, description);
}

void
BoundComputeKernel::
addConstraint(std::shared_ptr<const CommandExpression> lhs,
              const std::string & op,
              std::shared_ptr<const CommandExpression> rhs,
              const std::string & description)
{
    constraints.push_back({lhs, op, rhs, description});
}

namespace details {

MemoryArrayAbstractArgumentHandler::
MemoryArrayAbstractArgumentHandler(MemoryRegionHandle handle,
                                   std::shared_ptr<const ValueDescription> containedType,
                                   bool isConst)
    : handle(std::move(handle))
{
    ExcAssert(containedType);
    ComputeKernelType type;
    type.baseType = std::move(containedType);
    type.access = isConst ? ACC_READ : ACC_READ_WRITE;
    type.dims.push_back({nullptr});
    this->type = std::move(type);
    this->isConst = isConst;
}

bool
MemoryArrayAbstractArgumentHandler::
canGetRange() const
{
    return !isConst;
}

std::tuple<void *, size_t, std::shared_ptr<const void>>
MemoryArrayAbstractArgumentHandler::
getRange(const std::string & opName, ComputeContext & context) const
{
    auto region = context.transferToHostMutableSyncImpl(opName, handle);
    return { region.data(), region.length(), region.handle() };
}

bool
MemoryArrayAbstractArgumentHandler::
canGetConstRange() const
{
    return true;
}

std::tuple<const void *, size_t, std::shared_ptr<const void>>
MemoryArrayAbstractArgumentHandler::
getConstRange(const std::string & opName, ComputeContext & context) const
{
    auto region = context.transferToHostSyncImpl(opName, handle);
    return { region.data(), region.length(), region.handle() };
}

bool
MemoryArrayAbstractArgumentHandler::
canGetHandle() const
{
    return true;
}

MemoryRegionHandle
MemoryArrayAbstractArgumentHandler::
getHandle(const std::string & opName, ComputeContext & context) const
{
    return handle;
}

std::string
MemoryArrayAbstractArgumentHandler::
info() const
{
    size_t objectLength = type.baseType->width;
    if (objectLength % type.baseType->align != 0) {
        objectLength += type.baseType->align - objectLength % type.baseType->align;
    }

    ExcAssertEqual(objectLength % type.baseType->align, 0);

    size_t numObjects = handle.handle ? handle.handle->lengthInBytes / objectLength : 0;

    auto baseType = type;
    baseType.dims.clear();

    std::string result = baseType.print() + "[" + std::to_string(numObjects) + "]";
    if (handle.handle) result += " (as MemoryArrayHandle)";
    else result += " (NULL)";
    return result;
}

Json::Value
MemoryArrayAbstractArgumentHandler::
toJson() const
{
    size_t objectLength = type.baseType->width;
    ExcAssertEqual(objectLength % type.baseType->align, 0);
    size_t numObjects = handle.handle ? handle.handle->lengthInBytes / objectLength : 0;

    Json::Value result;
    result["elType"] = printBaseType(*type.baseType);
    result["elWidth"] = type.baseType->width;
    result["elAlign"] = type.baseType->align;
    result["length"] = numObjects;
    result["type"] = type.print();
    if (handle.handle) {
        result["name"] = handle.handle->name;
        result["version"] = handle.handle->version;
    }

    return result;
}

void
MemoryArrayAbstractArgumentHandler::
setFromReference(ComputeContext & context, std::span<const byte> referenceData)
{
    std::string opName = "setFromReference " + handle.handle->name;
    context.fillDeviceRegionFromHostSyncImpl(opName, handle, referenceData);
}


// PromiseAbstractArgumentHandler

bool
PromiseAbstractArgumentHandler::
canGetPrimitive() const
{
    return subImpl->canGetPrimitive();
}

std::span<const std::byte>
PromiseAbstractArgumentHandler::
getPrimitive(const std::string & opName, ComputeContext & context) const
{
    return subImpl->getPrimitive(opName, context);
}

bool
PromiseAbstractArgumentHandler::
canGetRange() const
{
    return subImpl->canGetRange();
}

std::tuple<void *, size_t, std::shared_ptr<const void>>
PromiseAbstractArgumentHandler::
getRange(const std::string & opName, ComputeContext & context) const
{
    return subImpl->getRange(opName, context);
}

bool
PromiseAbstractArgumentHandler::
canGetConstRange() const
{
    return subImpl->canGetConstRange();
}

std::tuple<const void *, size_t, std::shared_ptr<const void>>
PromiseAbstractArgumentHandler::
getConstRange(const std::string & opName, ComputeContext & context) const
{
    return subImpl->getConstRange(opName, context);
}

bool
PromiseAbstractArgumentHandler::
canGetHandle() const
{
    return subImpl->canGetHandle();
}

MemoryRegionHandle
PromiseAbstractArgumentHandler::
getHandle(const std::string & opName, ComputeContext & context) const
{
    return subImpl->getHandle(opName, context);
}

std::string
PromiseAbstractArgumentHandler::
info() const
{
    return subImpl->info();
}

Json::Value
PromiseAbstractArgumentHandler::
toJson() const
{
    return subImpl->toJson();
}

void
PromiseAbstractArgumentHandler::
setFromReference(ComputeContext & context, std::span<const byte> referenceData)
{
    subImpl->setFromReference(context, referenceData);
}


// PrimitiveAbstractArgumentHandler

bool
PrimitiveAbstractArgumentHandler::
canGetPrimitive() const
{
    return true;
}

std::span<const std::byte>
PrimitiveAbstractArgumentHandler::
getPrimitive(const std::string & opName, ComputeContext & context) const
{
    return mem;
}

std::string
PrimitiveAbstractArgumentHandler::
info() const
{
    return AbstractArgumentHandler::info() + " = " + this->type.baseType->printJsonString(mem.data()).rawString();
}
Json::Value
PrimitiveAbstractArgumentHandler::
toJson() const
{
    return this->type.baseType->printJsonStructured(mem.data());
}

void
PrimitiveAbstractArgumentHandler::
setFromReference(ComputeContext & context, std::span<const byte> referenceData)
{
    // TODO: how do we ensure it's pinned?
    this->mem = referenceData;
}

} // namespace details

} // namespace MLDB
