#include "compute_kernel_call_utils.h"
#include "mldb/types/generic_atom_description.h"
#include "mldb/types/enum_description.h"
#include "mldb/arch/demangle.h"
#include <memory>

namespace MLDB {
namespace details {

std::shared_ptr<ValueDescription>
makeGenericEnumDescription(std::shared_ptr<const ValueDescription> underlying, std::string typeName, const std::type_info * tinfo)
{
    return std::make_shared<GenericEnumDescription>(underlying, typeName, tinfo);
}

std::shared_ptr<ValueDescription>
makeGenericAtomDescription(size_t width, size_t align, std::string typeName, const std::type_info * tinfo)
{
    return std::make_shared<GenericAtomDescription>(width, align, typeName, tinfo);
}

} // namespace details
} // namespace MLDB
