#include "compute_kernel.h"
#include "mldb/base/parse_context.h"
#include "mldb/types/value_description.h"
#include "mldb/types/structure_description.h"
#include "mldb/types/vector_description.h"
#include "mldb/types/enum_description.h"
#include "mldb/types/meta_value_description.h"
#include "mldb/types/map_description.h"
#include "mldb/types/set_description.h"
#include "mldb/utils/command_expression.h"
#include <memory>

using namespace std;

namespace MLDB {

using namespace PluginCommand;

DEFINE_ENUM_DESCRIPTION_INLINE(ComputeKernelOrdering)
{
    addValue("ORDERED", ComputeKernelOrdering::ORDERED, "Array is ordered along this dimension");
    addValue("UNORDERED", ComputeKernelOrdering::UNORDERED, "Array is not ordered along this dimension");
}

DEFINE_STRUCTURE_DESCRIPTION_INLINE(ComputeKernelDimension)
{
    addField("tight", &ComputeKernelDimension::tight, "");
    addField("bound", &ComputeKernelDimension::bound, "");
    addField("ordering", &ComputeKernelDimension::ordering, "");
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
    addValue("READ", ACC_READ, "Read-only access");
    addValue("WRITE", ACC_WRITE, "Write-only access");
    addValue("READ_WRITE", ACC_READ_WRITE, "Read/write access");
}

DEFINE_STRUCTURE_DESCRIPTION_INLINE(ComputeKernelConstraint)
{
    addField("lhs", &ComputeKernelConstraint::lhs, "Left side of constraint expression");
    addField("op", &ComputeKernelConstraint::op, "Comparison operator for constraint");
    addField("rhs", &ComputeKernelConstraint::rhs, "Right side of constraint expression");
    addField("description", &ComputeKernelConstraint::description, "Description of where constraint comes from");
}

DEFINE_STRUCTURE_DESCRIPTION_INLINE(ComputeKernelConstraintSet)
{
    addField("constraints", &ComputeKernelConstraintSet::constraints, "List of constraints");
}

DEFINE_STRUCTURE_DESCRIPTION_INLINE(ComputeKernelConstraintSolution)
{
    addField("knowns", &ComputeKernelConstraintSolution::knowns, &CommandExpressionVariables::values, "Known values");
    addField("unknowns", &ComputeKernelConstraintSolution::unknowns, "Set of unknown values");
    addField("unknownFunctions", &ComputeKernelConstraintSolution::unknownFunctions, "Set of unknown functions");
}

DEFINE_STRUCTURE_DESCRIPTION_INLINE(ComputeTuneable)
{
    addField("name", &ComputeTuneable::name, "Name of tuneable parameter");
    addField("defaultValue", &ComputeTuneable::defaultValue, "Default value of tuneable parameter");
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
REGISTER_BASIC_TYPE(half, "f16");
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
        context.skip_whitespace();
        bool tight = context.match_literal('=');
        context.skip_whitespace();
        auto bound = CommandExpression::parseArgumentExpression(context);
        auto ordering = ComputeKernelOrdering::ORDERED;
        if (context.match_literal(":")) {
            if (context.match_literal("unordered"))
                ordering = ComputeKernelOrdering::UNORDERED;
        }
        context.expect_literal(']', "expected closing array expression");
        result.dims.push_back({tight, bound, ordering});
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

    if (!simd.empty()) {
        if (simd.size() != 1 || simd[0] != 1) {
            std::string simdStr = "v" + std::to_string(simd[0]);
            for (size_t i = 1;  i < simd.size();  ++i)
                simdStr += "x";

            typeName = simdStr + " " + typeName;
        }
    }

    auto result = printAccess(access) + " " + typeName;
    for (auto [tight, bound, ordering]: dims) {
        result += string("[") + (tight ? "=" : "") + (bound ? bound->surfaceForm : std::string());
        if (ordering == ComputeKernelOrdering::UNORDERED)
            result += ":unordered";
        result += "]";
    }
    return result;
}

static bool
typesAreCompatible(const ValueDescription & passed,
                   const ValueDescription & expected,
                   const std::string & context,
                   bool strict,
                   std::string * reason = nullptr)
{
    auto fail = [&] (std::string why) -> bool { if (reason) *reason = context + ": " + why;  return false; };
    auto failCompare = [&] (std::string what, const auto & passed, const auto & expected) -> bool
    {
        return fail(what + " not equal: passed value '" + jsonEncodeStr(passed) + "' != expected value '" + jsonEncodeStr(expected));
    };

    if (passed.kind != expected.kind) {
        auto isAtomic = [] (auto kind)
        { 
            return kind == ValueKind::ATOM || kind == ValueKind::BOOLEAN || kind == ValueKind::INTEGER || kind == ValueKind::FLOAT;
        };

        if (!isAtomic(passed.kind) && !isAtomic(expected.kind))
            return failCompare("kind", passed.kind, expected.kind);
    }    

    switch (passed.kind) {
    case ValueKind::ATOM:
    case ValueKind::INTEGER:
    case ValueKind::FLOAT:
    case ValueKind::BOOLEAN:
        break;

    case ValueKind::ENUM:
        if (!typesAreCompatible(passed.contained(), expected.contained(), context + ".(enum underlying)", strict, reason))
            return false;
        break;

    case ValueKind::ARRAY: {
        if (passed.getArrayLengthModel() != LengthModel::FIXED || expected.getArrayLengthModel() != LengthModel::FIXED)
            return fail("arrays must be fixed length");
        if (passed.getArrayIndirectionModel() != OwnershipModel::NONE || expected.getArrayIndirectionModel() != OwnershipModel::NONE)
            return fail("arrays must have inline elements (NONE ownership model)");
        if (passed.getArrayFixedLength() != passed.getArrayFixedLength())
            return failCompare("array length", passed.getArrayFixedLength(), passed.getArrayFixedLength());
        
        if (!typesAreCompatible(passed.contained(), expected.contained(), context + ".(fixed array element)", true /* strict */, reason))
            return false;
        break;
    }

    case ValueKind::STRUCTURE: {
        if (expected.kind != ValueKind::STRUCTURE)
            return fail("passed kind was structure but expected kind was not");
        
        if (!passed.hasFixedFieldCount() || !expected.hasFixedFieldCount())
            return fail("arrays must have fixed field counts");

        if (passed.getFixedFieldCount() != expected.getFixedFieldCount()) {
            return failCompare("structure field count", passed.getFixedFieldCount(), expected.getFixedFieldCount());            
        }

        for (size_t i = 0, n = passed.getFixedFieldCount();  i < n;  ++i) {
            auto & passedField = passed.getFieldByNumber(i);
            auto & expectedField = expected.getFieldByNumber(i);

            if (false && passedField.fieldName != expectedField.fieldName) {
                return failCompare("structure field " + std::to_string(i) + " name",
                                   passedField.fieldName, expectedField.fieldName);
            }

            if (passedField.offset != expectedField.offset) {
                return failCompare("structure field " + passedField.fieldName + " name",
                                   passedField.offset, expectedField.offset);
            }

            if (!typesAreCompatible(*passedField.description, *expectedField.description,
                                    context + "." + passedField.fieldName, true /* strict */, reason))
                return false;                           
        }
        break;
    }

    case ValueKind::STRING:
    case ValueKind::OPTIONAL:
    case ValueKind::LINK:
    case ValueKind::TUPLE:
    case ValueKind::VARIANT:
    case ValueKind::MAP:
    case ValueKind::ANY:
        return fail("kind " + jsonEncodeStr(passed.kind) + " not suitable for compute kernels");

    default:
        throw MLDB::Exception("Unknown value kind checking compatibility");
    }

    if (strict) {
        if (passed.width != expected.width)
            return failCompare("fundamental type width", passed.width, expected.width);
        if (passed.align != expected.align)
            return failCompare("fundamental type alignment", passed.width, expected.width);
    }

    return true;
}

bool
ComputeKernelType::
isCompatibleWith(const ComputeKernelType & otherType, std::string * reason) const
{
    auto fail = [&] (std::string why) -> bool { if (reason) *reason = std::move(why);  return false; };

    if (!baseType || !otherType.baseType)
        return fail("base types are not completely specified: return " + print() + " vs passed " + otherType.print());

    if (!typesAreCompatible(*baseType, *otherType.baseType, "argument", false /* strict */, reason))
        return false;

    //if (baseType->typeName != otherType.baseType->typeName) {
    //    return fail("return type " + printBaseType(*baseType)
    //                + " not same as passed type " + printBaseType(*otherType.baseType));
    //}

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

// ComputeEvent

std::string
ComputeEvent::
label() const
{
    return "";
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
            return { std::move(region), this->makeAlreadyResolvedEvent(opName + "then enqueueFillArray::INIT_NONE") };  // nothing to do 
        case MemoryRegionInitialization::INIT_ZERO_FILLED: {
            auto kernel = owner->getKernel("__zeroFillArray");
            auto bound = kernel->bind("region", region,
                                      "startOffsetInBytes", (uint64_t)startOffsetInBytes,
                                      "lengthInBytes", (uint64_t)lengthInBytes);
            enqueue(opName, bound, {} /* grid */);
            finish();
            return { std::move(region), this->makeAlreadyResolvedEvent(opName + "then enqueueFillArray::INIT_ZERO_FILLED") };
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
            enqueue(opName, bound, {} /* grid */);
            auto ev = flush();
            return { std::move(region), ev };
        }
    }
    throw MLDB::Exception("Unknown fillArray implementation");
}

FrozenMemoryRegion
ComputeQueue::
transferToHostSyncImpl(const std::string & opName,
                       MemoryRegionHandle handle)
{
    return owner->transferToHostSyncImpl(opName, std::move(handle));
}


// ComputeMarker

std::shared_ptr<ComputeMarker>
ComputeMarker::
enterScope(const std::string & scopeName)
{
    return std::make_shared<ComputeMarker>();
}


// ComputeContext

std::any
ComputeContext::
getCacheEntry(const std::string & key) const
{
    std::unique_lock guard(cacheMutex);
    auto it = cache.find(key);
    if (it == cache.end()) {
        return std::any();
    }
    return it->second;
}

std::any
ComputeContext::
setCacheEntry(const std::string & key, std::any value)
{
    std::unique_lock guard(cacheMutex);
    std::any oldValue;
    auto it = cache.find(key);
    if (it == cache.end()) {
        cache.emplace(key, std::move(value));
        return oldValue;
    }
    oldValue = std::move(it->second);
    it->second = std::move(value);
    return oldValue;
}

std::shared_ptr<ComputeMarker>
ComputeContext::
getScopedMarker(const std::string & scopeName)
{
    // No-op function
    return std::make_shared<ComputeMarker>();
}

void
ComputeContext::
recordMarkerEvent(const std::string & event)
{
    // no-op
}

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

std::set<std::string> getFunctions(const CommandExpression & expression)
{
    std::set<std::string> result;
    auto * var = dynamic_cast<const FunctionExpression *>(&expression);
    if (var) {
        result = { var->functionNameValue };
    }
    else {
        for (auto & clause: expression.childClauses()) {
            ExcAssert(clause);
            auto fns = getFunctions(*clause);
            result.insert(fns.begin(), fns.end());
        }
    }
    return result;
}

bool constraintSatisfied(const std::string & op, const Json::Value & lhsVal, const Json::Value & rhsVal)
{
    bool satisfied;
    if (op == "==") {
        satisfied = lhsVal == rhsVal;
    }
    else if (op == "!=") {
        satisfied = lhsVal != rhsVal;
    }
    else if (op == "<=") {
        satisfied = lhsVal <= rhsVal;
    }
    else if (op == "<") {
        satisfied = lhsVal < rhsVal;
    }
    else if (op == ">=") {
        satisfied = lhsVal >= rhsVal;
    }
    else if (op == ">") {
        satisfied = lhsVal > rhsVal;
    }
    else throw MLDB::Exception("Unknown constraint operation: " + op);
    return satisfied;
}

} // file scope

ComputeKernelConstraint::
ComputeKernelConstraint(std::shared_ptr<const CommandExpression> lhsIn,
                        std::string opIn,
                        std::shared_ptr<const CommandExpression> rhsIn,
                        std::string descriptionIn)
    : lhs(std::move(lhsIn)),
      op(std::move(opIn)),
      rhs(std::move(rhsIn)),
      description(std::move(descriptionIn))
{
    lhsVariables = getVariables(*lhs);
    lhsFunctions = getFunctions(*lhs);
    rhsVariables = getVariables(*rhs);
    rhsFunctions = getFunctions(*rhs);
    hash = std::hash<std::string>()(print());
}

std::string
ComputeKernelConstraint::
print() const
{
    return lhs->surfaceForm + op + rhs->surfaceForm + " (" + description + ")";
}

bool
ComputeKernelConstraint::
attemptToSatisfy(ComputeKernelConstraintSolution & solution) const
{
    if (solution.satisfied.count(hash))
        return false;

    bool canEvaluateLhs = true;
    for (auto var: lhsVariables) {
        if (!solution.hasValue(var)) {
            canEvaluateLhs = false;
            solution.unknowns.insert(var);
        }
    }
    for (auto fn: lhsFunctions) {
        if (!solution.hasFunction(fn)) {
            canEvaluateLhs = false;
            solution.unknownFunctions.insert(fn);
        }
    }

    Json::Value lhsVal;
    if (canEvaluateLhs) {
        lhsVal = solution.evaluate(*lhs);
    }

    bool canEvaluateRhs = true;
    for (auto var: rhsVariables) {
        if (!solution.hasValue(var)) {
            canEvaluateRhs = false;
            solution.unknowns.insert(var);
        }
    }
    for (auto fn: rhsFunctions) {
        if (!solution.hasFunction(fn)) {
            canEvaluateRhs = false;
            solution.unknownFunctions.insert(fn);
        }
    }

    Json::Value rhsVal;
    if (canEvaluateRhs) {
        rhsVal = solution.evaluate(*rhs);
    }
    
    if (!canEvaluateLhs && !canEvaluateRhs) {
        return false;
    }
    else if (canEvaluateLhs && canEvaluateRhs) {
        // An inequality... verify it
        bool satisfied = constraintSatisfied(op, lhsVal, rhsVal);
        if (!satisfied) {
            cerr << "op = " << op << endl;
            cerr << "lhsVal.type() = " << lhsVal.type() << endl;
            cerr << "rhsVal.type() = " << rhsVal.type() << endl;
            cerr << "lhsVal = " << lhsVal << endl;
            cerr << "rhsVal = " << rhsVal << endl;
            cerr << "lhs = " << lhs->surfaceForm << endl;
            cerr << "rhs = " << rhs->surfaceForm << endl;
            cerr << "solution = " << jsonEncode(solution) << endl;
            throw MLDB::Exception("Couldn't meet constraint " + print()
                                  + ": !(" + lhsVal.toStringNoNewLine()
                                  + op + rhsVal.toStringNoNewLine() + ")");
        }

        solution.satisfied.insert(hash);

        if (op != "==")
            return false;
    }

    const VariableExpression * lhsVar = dynamic_cast<const VariableExpression *>(lhs.get());
    const VariableExpression * rhsVar = dynamic_cast<const VariableExpression *>(rhs.get());

    if (lhsVar && canEvaluateRhs) {
        solution.unknowns.erase(lhsVar->variableName);
        if (!solution.hasValue(lhsVar->variableName)) {
            solution.setValue(lhsVar->variableName, rhsVal);
            solution.satisfied.insert(hash);
            return true;
        }
    }
    else if (rhsVar && canEvaluateLhs) {
        solution.unknowns.erase(rhsVar->variableName);
        if (!solution.hasValue(rhsVar->variableName)) {
            solution.setValue(rhsVar->variableName, lhsVal);
            solution.satisfied.insert(hash);
            return true;
        }
    }

    return false;
}

bool
ComputeKernelConstraint::
satisfied(const ComputeKernelConstraintSolution & solution) const
{
    if (solution.satisfied.count(hash))
        return true;

    CommandExpressionContext context(&solution.knowns);

    auto unknownLhs = lhs->unknowns(context);
    auto unknownRhs = rhs->unknowns(context);

    if (!unknownLhs.empty() || !unknownRhs.empty())
        return false;

    auto lhsVal = lhs->apply(context);
    auto rhsVal = rhs->apply(context);

    if (!constraintSatisfied(op, lhsVal, rhsVal)) {
        throw MLDB::Exception("constraint is unsatisfiable: " + print() + ": "
                              + lhsVal.toStringNoNewLine() + " != " + rhsVal.toStringNoNewLine());
    }

    return true;
}


// ComputeKernelConstraintSolution

bool
ComputeKernelConstraintSolution::
hasValue(const std::string & variableName) const
{
    return knowns.hasValue(variableName);
}

void
ComputeKernelConstraintSolution::
setValue(const std::string & variableName, const void * val, const ValueDescription & desc)
{
    setValue(variableName, desc.printJsonStructured(val));
}

void
ComputeKernelConstraintSolution::
setValue(const std::string & variableName, const Json::Value & val)
{
    knowns.setValue(variableName, val);
    unknowns.erase(variableName);
}

Json::Value
ComputeKernelConstraintSolution::
getValue(const std::string & variableName) const
{
    return knowns.getValue(variableName);
}

Json::Value
ComputeKernelConstraintSolution::
evaluate(const CommandExpression & expr) const
{
    try {
        CommandExpressionContext context(&knowns);
        return expr.apply(context);
    } MLDB_CATCH_ALL {
        rethrowException(500, "Error evaluating expression: " + getExceptionString(),
                         "expression", expr.surfaceForm, "knowns", knowns.values);
    }
}

bool
ComputeKernelConstraintSolution::
hasFunction(const std::string & functionName) const
{
    return knowns.hasFunction(functionName);
}

// ComputeKernelConstraintSet

void
ComputeKernelConstraintSet::
add(ComputeKernelConstraint constraint)
{
    constraints.emplace_back(std::move(constraint));
}

void
ComputeKernelConstraintSet::
assertSolvedBy(const ComputeKernelConstraintSolution & solution) const
{
    if (!solution.unknowns.empty()) {
        throw MLDB::Exception("Constraints do not solve for variable(s): " + jsonEncodeStr(solution.unknowns));
    }

    for (auto & c: constraints) {
        if (!c.satisfied(solution))
            throw MLDB::Exception("Constraint not satisfied: " + c.print());
    }
}

ComputeKernelConstraintSolution
solve(const ComputeKernelConstraintSolution & solutionIn,
      const ComputeKernelConstraintSet & constraints)
{
    return solve(solutionIn, constraints, {});
}

ComputeKernelConstraintSolution
solve(const ComputeKernelConstraintSolution & solutionIn,
      const ComputeKernelConstraintSet & constraints1,
      const ComputeKernelConstraintSet & constraints2)
{
    Timer timer;

    ComputeKernelConstraintSolution result = solutionIn;

    bool progress = true;

    while (progress) {
        progress = false;
        for (auto & c: constraints1.constraints) {
            progress = progress || c.attemptToSatisfy(result);
        }
        for (auto & c: constraints2.constraints) {
            progress = progress || c.attemptToSatisfy(result);
        }
    }

    cerr << "solving with " << constraints1.constraints.size() + constraints2.constraints.size()
         << " constraints took " << timer.elapsed_wall() * 1000000.0 << "us" << endl;

    return result;
}

ComputeKernelConstraintSolution
fullySolve(const ComputeKernelConstraintSolution & solutionIn,
           const ComputeKernelConstraintSet & constraints)
{
    return fullySolve(solutionIn, constraints, {});
}

ComputeKernelConstraintSolution
fullySolve(const ComputeKernelConstraintSolution & solutionIn,
           const ComputeKernelConstraintSet & constraints1,
           const ComputeKernelConstraintSet & constraints2)
{
    auto result = solve(solutionIn, constraints1, constraints2);
    constraints1.assertSolvedBy(result);
    constraints2.assertSolvedBy(result);
    return result;
}

// ComputeKernelConstraintManager

void
ComputeKernelConstraintManager::
addConstraint(const std::string lhs, const std::string & op, const std::string & rhs,
              const std::string & description,
              ComputeKernelConstraintScope scope)
{
    auto lhsParsed = CommandExpression::parseArgumentExpression(lhs);
    auto rhsParsed = CommandExpression::parseArgumentExpression(rhs);
    addConstraint(lhsParsed, op, rhsParsed, description, scope);
}

void
ComputeKernelConstraintManager::
addConstraint(std::shared_ptr<const CommandExpression> lhs,
              const std::string & op, const std::string & rhs,
              const std::string & description,
              ComputeKernelConstraintScope scope)
{
    auto rhsParsed = CommandExpression::parseArgumentExpression(rhs);
    addConstraint(lhs, op, rhsParsed, description, scope);
}

void
ComputeKernelConstraintManager::
addConstraint(std::shared_ptr<const CommandExpression> lhs,
              const std::string & op,
              std::shared_ptr<const CommandExpression> rhs,
              const std::string & description,
              ComputeKernelConstraintScope scope)
{
    switch (scope) {
    case ComputeKernelConstraintScope::PRE_RUN:
        preConstraints.add({lhs, op, rhs, description});
        break;
    case ComputeKernelConstraintScope::POST_RUN:
        postConstraints.add({lhs, op, rhs, description});
        break;
    case ComputeKernelConstraintScope::UNIVERSAL:
        constraints.add({lhs, op, rhs, description});
        break;
    }
}


// ComputeKernel



// BoundComputeKernel


void
BoundComputeKernel::
setKnownsFromArguments()
{
    for (auto & arg: arguments) {
        if (arg.handler && !knowns.hasValue(arg.name)) {
            // Was set by the caller
            knowns.setValue(arg.name, arg.handler->toJson());
        }
    }

    for (auto & [name, value]: tuneables) {
        knowns.setValue(name, value);
    }
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
    type.dims.push_back({false, nullptr});
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

Json::Value
MemoryArrayAbstractArgumentHandler::
getArrayElement(uint32_t index, ComputeContext & context) const
{
    if (!handle.handle)
        throw MLDB::Exception("GetArrayElement: no handler");
    size_t objectLength = type.baseType->width;
    ExcAssertEqual(objectLength % type.baseType->align, 0);
    size_t numObjects = handle.handle ? handle.handle->lengthInBytes / objectLength : 0;
    ExcAssertLess(index, numObjects);

    auto region = context.transferToHostSyncImpl("getArrayElement", handle);
    ExcAssert(type.baseType);
    const ValueDescription & desc = *type.baseType;

    const char * data = region.data() + desc.width * index;

    return desc.printJsonStructured(data);
}

void
MemoryArrayAbstractArgumentHandler::
setFromReference(ComputeContext & context, std::span<const byte> referenceData)
{
    std::string opName = "setFromReference " + handle.handle->name;
    context.fillDeviceRegionFromHostSyncImpl(opName, handle, referenceData);
}

#if 0

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

Json::Value
PromiseAbstractArgumentHandler::
getArrayElement(uint32_t index, ComputeContext & context) const
{
    return subImpl->getArrayElement(index, context);
}

void
PromiseAbstractArgumentHandler::
setFromReference(ComputeContext & context, std::span<const byte> referenceData)
{
    subImpl->setFromReference(context, referenceData);
}

#endif

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

Json::Value
PrimitiveAbstractArgumentHandler::
getArrayElement(uint32_t index, ComputeContext & context) const
{
    cerr << "getting element " << index << " from " << toJson() << endl;
    const void * el = this->type.baseType->getArrayElement(mem.data(), index);
    return this->type.baseType->getArrayElementDescription(mem.data(), index).printJsonStructured(el);
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
