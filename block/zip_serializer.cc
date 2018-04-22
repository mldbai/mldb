/** zip_serialier.cc                                               -*- C++ -*-
    Jeremy Barnes, 27 March 2016
    This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.

    Implementation of code to freeze columns into a binary format.
*/

#include "zip_serializer.h"
#include "memory_region_impl.h"
#include "http/http_exception.h"
#include "mldb/vfs/filter_streams.h"
#include "mldb/arch/timers.h"

// libarchive support
#include "mldb/ext/libarchive/libarchive/archive.h"
#include "mldb/ext/libarchive/libarchive/archive_entry.h"


using namespace std;

namespace MLDB {


/*****************************************************************************/
/* ZIP STRUCTURED SERIALIZER                                                 */
/*****************************************************************************/

struct ZipStructuredSerializer::Itl {
    virtual ~Itl()
    {
    }

    virtual void commit() = 0;

    virtual Path path() const = 0;

    virtual BaseItl * base() const = 0;
};

struct ZipStructuredSerializer::BaseItl: public Itl {
    BaseItl(Utf8String filename)
    {
        stream.open(filename.rawString());
        a.reset(archive_write_new(),
                [] (struct archive * a) { archive_write_free(a); });
        if (!a.get()) {
            throw HttpReturnException
                (500, "Couldn't create archive object");
        }
        archive_op(archive_write_set_format_zip);

        // We compress each one individually using zstandard or not
        // at all if we want to mmap.
        archive_op(archive_write_zip_set_compression_store);
        archive_op(archive_write_set_bytes_per_block, 65536);
        archive_op(archive_write_open, this,
                   &BaseItl::openCallback,
                   &BaseItl::writeCallback,
                   &BaseItl::closeCallback);
    }

    ~BaseItl()
    {
        cerr << "closing archive file" << endl;
        archive_op(archive_write_close);
    }
    
    // Perform a libarchive operation
    template<typename Fn, typename... Args>
    void archive_op(Fn&& op, Args&&... args)
    {
        int res = op(a.get(), std::forward<Args>(args)...);
        if (res != ARCHIVE_OK) {
            throw HttpReturnException
                (500, string("Error writing zip file: ")
                 + archive_error_string(a.get()));
        }
    }

    struct Entry {
        Entry()
        {
            entry.reset(archive_entry_new(),
                        [] (archive_entry * e) { archive_entry_free(e); });
            if (!entry.get()) {
                throw HttpReturnException
                    (500, "Couldn't create archive entry");
            }
        }

        template<typename Fn, typename... Args>
        void op(Fn&& op, Args&&... args)
        {
            /*int res =*/ op(entry.get(), std::forward<Args>(args)...);
            /*
            if (res != ARCHIVE_OK) {
                throw HttpReturnException
                    (500, string("Error writing zip file: ")
                     + archive_error_string(entry.get()));
            }
            */
        }
        
        std::shared_ptr<struct archive_entry> entry;
    };

    void writeEntry(Path name, FrozenMemoryRegion region)
    {
        Entry entry;
        entry.op(archive_entry_set_pathname, name.toUtf8String().rawData());
        entry.op(archive_entry_set_size, region.length());
        entry.op(archive_entry_set_filetype, AE_IFREG);
        entry.op(archive_entry_set_perm, 0440);
#if 0
        for (auto & a: attrs) {
            entry.op(archive_entry_xattr_add_entry, a.first.c_str(),
                     a.second.data(), a.second.length());
        }
#endif
        archive_op(archive_write_header, entry.entry.get());
        auto written = archive_write_data(a.get(), region.data(), region.length());
        if (written != region.length()) {
            throw HttpReturnException(500, "Not all data written");
        }
    }

    virtual void commit()
    {
    }

    virtual Path path() const
    {
        return Path();
    }

    virtual BaseItl * base() const
    {
        return const_cast<BaseItl *>(this);
    }
    
    static int openCallback(struct archive * a, void * voidThis)

    {
        // Nothing to do here
        return ARCHIVE_OK;
    }

    static int closeCallback(struct archive * a, void * voidThis)
    {
        // Nothing to do here
        cerr << "close callback" << endl;
        return ARCHIVE_OK;
    }

    /** Callback from libarchive when it needs to write some data to the
        output file.  This simply hooks it into the filter ostream.
    */
    static __LA_SSIZE_T	writeCallback(struct archive * a,
                                      void * voidThis,
                                      const void * buffer,
                                      size_t length)
    {
        BaseItl * that = reinterpret_cast<BaseItl *>(voidThis);

        //cerr << "need to write " << archive_write_get_bytes_in_last_block(that->a.get()) << endl;

        that->stream.write((const char *)buffer, length);
        if (!that->stream)
            return -1;
        //cerr << "wrote " << length << " characters" << endl;
        return length;
    }

    filter_ostream stream;
    std::shared_ptr<struct archive> a;
};

struct ZipStructuredSerializer::RelativeItl: public Itl {
    RelativeItl(Itl * parent,
                PathElement relativePath)
        : base_(parent->base()), parent(parent), relativePath(relativePath)
    {
    }

    virtual void commit()
    {
        cerr << "commiting " << path() << endl;
        // nothing to do; each entry will have been committed as it
        // was written
    }

    virtual Path path() const
    {
        return parent->path() + relativePath;
    }

    virtual BaseItl * base() const
    {
        return base_;
    }

    BaseItl * base_;
    Itl * parent;
    PathElement relativePath;
};

struct ZipStructuredSerializer::EntrySerializer: public MemorySerializer {
    EntrySerializer(Itl * itl, PathElement entryName)
        : itl(itl), entryName(std::move(entryName))
    {
    }

    virtual ~EntrySerializer()
    {
        Path name = itl->path() + entryName;
        //cerr << "finishing entry " << name << " with "
        //     << frozen.length() << " bytes" << endl;
        itl->base()->writeEntry(name, std::move(frozen));
    }

    virtual void commit() override
    {
    }

    virtual FrozenMemoryRegion freeze(MutableMemoryRegion & region) override
    {
        return frozen = MemorySerializer::freeze(region);
    }

    Itl * itl;
    PathElement entryName;
    FrozenMemoryRegion frozen;
};

ZipStructuredSerializer::
ZipStructuredSerializer(Utf8String filename)
    : itl(new BaseItl(filename))
{
}

ZipStructuredSerializer::
ZipStructuredSerializer(ZipStructuredSerializer * parent,
                        PathElement relativePath)
    : itl(new RelativeItl(parent->itl.get(), relativePath))
{
}

ZipStructuredSerializer::
~ZipStructuredSerializer()
{
}

std::shared_ptr<StructuredSerializer>
ZipStructuredSerializer::
newStructure(const PathElement & name)
{
    return std::make_shared<ZipStructuredSerializer>(this, name);
}

std::shared_ptr<MappedSerializer>
ZipStructuredSerializer::
newEntry(const PathElement & name)
{
    return std::make_shared<EntrySerializer>(itl.get(), name);
}

filter_ostream
ZipStructuredSerializer::
newStream(const PathElement & name)
{
    auto entry = newEntry(name);

    auto handler = std::make_shared<SerializerStreamHandler>();
    handler->owner = entry.get();
    handler->baggage = entry;

    filter_ostream result;
    result.openFromStreambuf(handler->stream.rdbuf(), handler);
    
    return result;
}

void
ZipStructuredSerializer::
commit()
{
    itl->commit();
}


/*****************************************************************************/
/* ZIP STRUCTURED RECONSTITUTER                                              */
/*****************************************************************************/

struct ZipStructuredReconstituter::Itl {

    // Zip file entry
    struct Entry {
        Path path;
        std::map<PathElement, Entry> children;
        FrozenMemoryRegion region;
    };

    const Entry * root = nullptr;
    Entry rootStorage;

    Itl(const Entry * root)
        : root(root)
    {
    }
    
    Itl(FrozenMemoryRegion region_)
        : region(std::move(region_))
    {
        Timer timer;
        
        a.reset(archive_read_new(),
                [] (struct archive * a) { archive_read_free(a); });
        if (!a.get()) {
            throw HttpReturnException
                (500, "Couldn't create archive object");
        }
        archive_op(archive_read_support_format_zip);
        archive_op(archive_read_open_memory, (void *)region.data(),
                   region.length());
        
        // Read the archive header
        archive_entry * entry = nullptr;
        
        // Read the entire zip file directory, and index where the files are
        while (archive_op(archive_read_next_header, &entry)) {
            const void * currentBlock;
            size_t length;
            __LA_INT64_T currentOffset;
            archive_read_data_block
                (a.get(), &currentBlock, &length, &currentOffset);
            ssize_t offset = (const char *)currentBlock - region.data();
            if (offset >= 0 && offset <= region.length()) {
                //cerr << "calculated offset = " << offset << endl;
            }
            else if (length == 0) {
                offset = 0;
            }
            else {
                // TODO: take decompressed data, or find something else to do
                cerr << "length = " << length << endl;
                cerr << "calculated offset = " << offset << endl;
                ExcAssert(false);
            }

            Path path = Path::parse(archive_entry_pathname(entry));

            // Insert into index
            Entry * current = &rootStorage;
            
            for (auto e: path) {
                current = &current->children[e];
            }
            
            current->path = std::move(path);
            current->region = this->region.range(offset, offset + length);
        }

        this->root = &rootStorage;

        cerr << "reading Zip entries took " << timer.elapsed() << endl;
    }

    // Perform a libarchive operation
    template<typename Fn, typename... Args>
    bool archive_op(Fn&& op, Args&&... args)
    {
        int res = op(a.get(), std::forward<Args>(args)...);
        if (res == ARCHIVE_EOF)
            return false;
        if (res != ARCHIVE_OK) {
            throw HttpReturnException
                (500, string("Error reading zip file: ")
                 + archive_error_string(a.get()));
        }
        return true;
    }
    
    FrozenMemoryRegion region;
    ssize_t currentOffset = 0;
    std::shared_ptr<struct archive> a;
};

ZipStructuredReconstituter::
ZipStructuredReconstituter(const Url & path)
    : itl(new Itl(mapFile(path)))
{
}

ZipStructuredReconstituter::
ZipStructuredReconstituter(FrozenMemoryRegion buf)
    : itl(new Itl(std::move(buf)))
{
}

ZipStructuredReconstituter::
ZipStructuredReconstituter(Itl * itl)
    : itl(itl)
{
}

ZipStructuredReconstituter::
~ZipStructuredReconstituter()
{
}
    
Utf8String
ZipStructuredReconstituter::
getContext() const
{
    return "zip://<some file>/" + itl->root->path.toUtf8String();
}

std::vector<StructuredReconstituter::Entry>
ZipStructuredReconstituter::
getDirectory() const
{
    std::vector<StructuredReconstituter::Entry> result;
    result.reserve(itl->root->children.size());
    
    for (auto & ch: itl->root->children) {
        StructuredReconstituter::Entry entry;
        entry.name = ch.first;

        if (ch.second.region.data()) {
            FrozenMemoryRegion region = ch.second.region;
            entry.getBlock = [=] () { return region; };
        }

        if (!ch.second.children.empty()) {
            entry.getStructure = [=] ()
                {
                    return std::shared_ptr<ZipStructuredReconstituter>
                        (new ZipStructuredReconstituter(new Itl(&ch.second)));
                };
        }

        result.emplace_back(std::move(entry));
    }

    return result;
}

std::shared_ptr<StructuredReconstituter>
ZipStructuredReconstituter::
getStructure(const PathElement & name) const
{
    auto it = itl->root->children.find(name);
    if (it == itl->root->children.end()) {
        throw HttpReturnException
            (400, "Child structure " + name.toUtf8String() + " not found at "
             + itl->root->path.toUtf8String());
    }
    return std::shared_ptr<ZipStructuredReconstituter>
        (new ZipStructuredReconstituter(new Itl(&it->second)));
}

FrozenMemoryRegion
ZipStructuredReconstituter::
getRegion(const PathElement & name) const
{
    auto it = itl->root->children.find(name);
    if (it == itl->root->children.end()) {
        throw HttpReturnException
            (400, "Child structure " + name.toUtf8String() + " not found");
    }
    return it->second.region;
}

} // namespace MLDB
