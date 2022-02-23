/** generic_atom_description.cc                                       -*- C++ -*-
    Jeremy Barnes, 4 January 2015
    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

    Description of generic atom types
*/

#include "generic_atom_description.h"

#define THROW_NOT_SUPPORTED do { throw MLDB::Exception(std::string("Not supported: ") + __PRETTY_FUNCTION__ + " at " + __FILE__ + ":" + std::to_string(__LINE__)); } while (false)  // + " at " + __FILE__ + ":" + __LINE__)

namespace MLDB {

GenericAtomDescription::
GenericAtomDescription(size_t width,
                       size_t align,
                       const std::string & typeName,
                       const std::type_info * type)
    : ValueDescription(ValueKind::ATOM, type, width, align, typeName)
{
}

void
GenericAtomDescription::
parseJson(void * val,
                        JsonParsingContext & context) const
{
    THROW_NOT_SUPPORTED;
}

void
GenericAtomDescription::
printJson(const void * val,
                        JsonPrintingContext & context) const
{
    THROW_NOT_SUPPORTED;
}

bool
GenericAtomDescription::
isDefault(const void * val) const
{
    return false;
}

void
GenericAtomDescription::
setDefault(void * val) const
{
    THROW_NOT_SUPPORTED;
}

void
GenericAtomDescription::
copyValue(const void * from, void * to) const
{
    memcpy(to, from, width);
}

void
GenericAtomDescription::
moveValue(void * from, void * to) const
{
    memcpy(to, from, width);    
}

void
GenericAtomDescription::
swapValues(void * from, void * to) const
{
    std::byte tmp[width];
    memcpy(tmp, from, width);
    memcpy(from, to, width);
    memcpy(to, tmp, width);
}

void
GenericAtomDescription::
initializeDefault(void * mem) const
{
    THROW_NOT_SUPPORTED;    
}

void
GenericAtomDescription::
initializeCopy(void * mem, const void * other) const
{
    memcpy(mem, other, width);    
}

void
GenericAtomDescription::
initializeMove(void * mem, void * other) const
{
    memcpy(mem, other, width);    
}

void
GenericAtomDescription::
destruct(void * mem) const
{    
}

} // namespace MLDB

