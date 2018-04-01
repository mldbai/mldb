/** zip_serializer.h                                               -*- C++ -*-
    Jeremy Barnes, 27 March 2016
    This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.

    Structured serializer/reconstituter for zip files.
*/

#pragma once

#include "memory_region.h"


namespace MLDB {


/*****************************************************************************/
/* ZIP STRUCTURED SERIALIZER                                                 */
/*****************************************************************************/

/** Structured serializer that writes a zip file. */

struct ZipStructuredSerializer: public StructuredSerializer {
    ZipStructuredSerializer(Utf8String filename);
    ~ZipStructuredSerializer();

    virtual std::shared_ptr<StructuredSerializer>
    newStructure(const PathElement & name);

    virtual std::shared_ptr<MappedSerializer>
    newEntry(const PathElement & name);

    virtual filter_ostream
    newStream(const PathElement & name);

    virtual void commit();

    ZipStructuredSerializer(ZipStructuredSerializer * parent,
                            PathElement relativePath);
private:
    struct Itl;
    struct BaseItl;
    struct RelativeItl;
    struct EntrySerializer;
    std::unique_ptr<Itl> itl;
};


/*****************************************************************************/
/* ZIP STRUCTURED RECONSTITUTER                                              */
/*****************************************************************************/

struct ZipStructuredReconstituter: public StructuredReconstituter {

    ZipStructuredReconstituter(const Url & path);

    ZipStructuredReconstituter(FrozenMemoryRegion buf);
    
    virtual ~ZipStructuredReconstituter();
    
    virtual Utf8String getContext() const;
    
    virtual std::vector<Entry> getDirectory() const;

    virtual std::shared_ptr<StructuredReconstituter>
    getStructure(const PathElement & name) const;

    virtual FrozenMemoryRegion
    getRegion(const PathElement & name) const;

private:
    struct Itl;
    std::unique_ptr<Itl> itl;

    ZipStructuredReconstituter(Itl * itl);
};

} // namespace MLDB

