/* mmap.cc                                              -*- C++ -*-
   Jeremy Barnes, 6 November 2015
   Copyright (c) 2015 mldb.ai inc.  All rights reserved.

   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
*/

#include "mmap.h"
#include <string>
#include <vector>
#include <sstream>
#include "mldb/arch/ansi.h"
#include "mldb/utils/ostream_vector.h"
#include "mldb/vfs/compressibility.h"
#include "mldb/arch/format.h"
#include "mldb/arch/demangle.h"
#include "mldb/types/value_description.h"
#include <regex>
#include <map>
#include <set>
#include <bit>

using namespace std;

namespace MLDB {

std::shared_ptr<const ValueDescription> getDescriptionMaybe(const std::type_info * type)
{
    return ValueDescription::get(*type);
}

using namespace ansi;


const MappingOption<bool> TRACE("TRACE", false);  // Do we trace this serialization or not?
const MappingOption<bool> DEBUG("DEBUG", false);  // Do we debug the mapping or not?
const MappingOption<bool> VALIDATE("VALIDATE", false);  // Do we perform extra validation?
const MappingOption<bool> DUMP_MEMORY_MAP("DUMP_MEMORY_MAP", false);  // Do we dump a map of the whole mapping?
const MappingOption<bool> DUMP_TYPE_STATS("DUMP_TYPE_STATS", false);  // Do we dump stats on the types?

MappingOptionBase::MappingOptionBase(std::string name, std::any defValue)
    : name_(std::move(name)), defValue_(std::move(defValue))
{
}

MappingContext::LocalState::~LocalState()
{
    if (parent)
        parent->childExitScope(*this);
}

template<typename T>
std::string to_string(const std::vector<T> & vals)
{
    std::ostringstream stream;
    stream << vals;
    return stream.str();
}

void printStats(MappingContext::Stats & stats, bool dumpMemoryMap, bool dumpTypeStats);

inline std::regex operator"" _r ( const char * s, size_t )
{
    return std::regex(s);
}

static std::string typeName(const std::type_info * type, const std::shared_ptr<const ValueDescription> info, size_t arrayLen, MappingContext::Directness directness)
{
    std::string result = info ? info->typeName : demangle(type->name());
    if (arrayLen != MappingContext::SCALAR)
        result += "[" + std::to_string(arrayLen) + "]";
    if (directness == MappingContext::INDIRECT)
        result += " (ind)";

    static const std::vector<std::pair<std::regex, std::string>> replacements = {
        {"unsigned long int"_r, "u64"}, {"long int"_r, "i64"}, {"unsigned int"_r, "u32"}, {"int"_r, "i32"}, {"MLDB::"_r,""} };

    for (auto & [rex, rep]: replacements)
        result = std::regex_replace(result, rex, rep);

    return result;
}

struct MappingContext::Stats {
    struct Entry {
        const std::byte * start = nullptr;
        size_t offset = 0;
        size_t len = 0;
        const std::type_info * type = nullptr;
        std::shared_ptr<const ValueDescription> info = nullptr;
        Directness directness = DIRECT;
        size_t arrayLen = SCALAR;
        std::vector<std::string> path;
        size_t count = 0;  // if we're a summarized entry

        CompressibilityStats compressibility;
        uint64_t numZeroBits = 0;
        uint64_t numZeroBytes = 0;

        double zeroBitRatio() const { return len ? 0.125 * numZeroBits / len : 0.0; }
        double zeroByteRatio() const { return len ? 1.0 * numZeroBytes / len : 0.0; }
        double compressionRatio() const { return compressibility.compressionRatio; }

        bool operator < (const Entry & other) const
        {
            if (offset < other.offset) return true;
            if (offset > other.offset) return false;
            if (len > other.len) return true;
            if (len < other.len) return false;
            return path.size() < other.path.size();
        }

        void calcStats()
        {
            compressibility = calc_compressibility(start, len);
            numZeroBits = 0;
            numZeroBytes = 0;

            for (size_t i = 0;  i < len;  ++i) {
                uint8_t byte = (uint8_t)start[offset + i];
                numZeroBytes += (byte == 0);
                numZeroBits += 8 - std::popcount(byte);
            }
        }

        std::string typeName() const
        {
            if (!type) 
                return "";
            return MLDB::typeName(type, info, arrayLen, directness);
        }

        std::string pathStr(size_t startAt = 0) const
        {
            std::string result;
            if (startAt >= path.size())
                return result;
            result = path[startAt];

            for (size_t i = startAt + 1;  i < path.size();  ++i) {
                if (!path[i].starts_with('['))
                    result += ".";
                result += path[i];
            }

            return result;
        }

        std::string shortPathStr() const
        {
            if (path.size() < 2)
                return path.empty() ? "" : path[0];
            size_t startAt = path.size() - 1;
            while (startAt >= 0 && path[startAt].starts_with('['))
                --startAt;
            return pathStr(startAt);
        }

        std::string shortColoredString() const
        {
            return ansi_str_cyan() + shortPathStr() + ansi_str_reset() + ":"
                + ansi_str_green() + typeName() + ansi_str_reset();
        }

        std::string colorCompressibility() const
        {
            std::string result;
            if (compressionRatio() < 0.5)
                result = ansi_str_bright_red();
            else if (compressionRatio() < 0.8)
                result = ansi_str_red();
            else if (compressionRatio() < 0.9)
                result = "";
            else if (compressionRatio() < 0.95)
                result = ansi_str_green();
            else result = ansi_str_bright_green();

            result += format("%6zd/%6zd bytes %.2f%%cmp %.2f%%zb %.2f%%zB",
                             len, compressibility.bytesCompressed,
                             100.0 * compressionRatio(),
                             100.0 * zeroBitRatio(), 
                             100.0 * zeroByteRatio());

            result += ansi_str_reset();

            return result;
        }

        Entry & operator += (const Entry & other)
        {
            if (start == nullptr) {
                start = other.start;
                type = other.type;
                directness = other.directness;
            }

            len += other.len;
            compressibility += other.compressibility;
            numZeroBits += other.numZeroBits;
            numZeroBytes += other.numZeroBytes;
            count += other.count + (other.arrayLen == SCALAR ? 1 : other.arrayLen);

            return *this;
        }

        static const std::string header()
        {
            return "type                                                 start-end         len   0br   0Br   cmpR path";
        }

        std::string print() const
        {
            char buf[1024];
            ::snprintf(buf, 1024, "%-50s%08llx-%08llx %6zd %5.1f %5.1f %6.2f %s",
                    (std::string(path.size(), ' ') + MLDB::typeName(type, info, arrayLen, directness)).c_str(),
                    (unsigned long long)offset, (unsigned long long)offset + len, len,
                    100.0 * zeroBitRatio(),
                    100.0 * zeroByteRatio(),
                    100.0 * compressionRatio(),
                    pathStr().c_str());

            return buf;
        }

        static const std::string statsHeader()
        {
            return "type                                                            count     bytes       avg   0br   0Br   cmpR  %total";
        }

        std::string printStats(size_t totalBytes) const
        {
            char buf[1024];
            ::snprintf(buf, 1024, "%-60s%9zd %9zd %9.0f %5.1f %5.1f %6.2f%8.4f",
                    typeName().c_str(),
                    count, len, 1.0 * len / count,
                    100.0 * zeroBitRatio(),
                    100.0 * zeroByteRatio(),
                    100.0 * compressionRatio(),
                    100.0 * len / totalBytes);

            return buf;
        }
    };

    std::vector<Entry> entries;
};

void dumpAnnotated(const MappingContext::Stats & stats, std::ostream & stream);

void printStats(MappingContext::Stats & stats, bool dumpMemoryMap, bool dumpTypeStats)
{
    std::ostream & stream = std::cerr;

    if (!dumpMemoryMap && !dumpTypeStats)
        return;

    MappingContext::Stats::Entry allEntry;
    allEntry.offset = 0;
    allEntry.len = MAX_LIMIT;
    stats.entries.push_back(allEntry);

    std::sort(stats.entries.begin(), stats.entries.end());

    std::map<std::pair<const std::type_info *, MappingContext::Directness>, MappingContext::Stats::Entry> typeStats;
    size_t totalLength = 0;

    for (auto & e: stats.entries) {
        if (e.len == MAX_LIMIT)
            continue;
        totalLength = std::max(totalLength, e.offset + e.len);
        if (e.len == 0)
            continue;
        e.calcStats();
        if (!e.type)
            continue;
        typeStats[std::make_pair(e.type, e.directness)] += e;
        //cerr << e.print() << endl;
    }

    ExcAssert(stats.entries[0].len == MAX_LIMIT);
    stats.entries[0].len = totalLength;
    stats.entries[0].start = stats.entries.back().start;
    stats.entries[0].calcStats();

    if (dumpMemoryMap) {
        stream << underline << "Mapping with " << stats.entries.size() << " entries" << reset << endl;
        stream << reversed << MappingContext::Stats::Entry::header() << reset << endl;
        dumpAnnotated(stats, stream);
    }

    if (dumpTypeStats) {
        std::vector<std::pair<std::pair<const std::type_info *, MappingContext::Directness>, MappingContext::Stats::Entry> > sortedTypeStats(typeStats.begin(), typeStats.end());
        std::sort(sortedTypeStats.begin(), sortedTypeStats.end(), [&] (auto & s1, auto & s2) { return s1.second.len > s2.second.len; });

        stream << endl << underline << "stats for " << typeStats.size() << " types" << reset << endl;
        stream << reversed << MappingContext::Stats::Entry::statsHeader() << reset << endl;

        for (auto & [type, e]: sortedTypeStats) {
            stream << e.printStats(totalLength) << endl;
        }
    }
}

void dumpAnnotated(const MappingContext::Stats & stats, std::ostream & stream)
{
    const std::byte * data = stats.entries.at(0).start;

    // First, nest the structures so we don't repeat ourselves by printing a parent as well
    // as its children (which cover the same memory range).
    struct NestedEntry {
        size_t entryIndex = 0;
        size_t entryStart = MAX_LIMIT;
        size_t entryEnd = MAX_LIMIT;
        size_t len() const { return entryEnd - entryStart; }

        bool isTopLevel() const
        {
            return childrenOverlap() || (children.size() == 1
                                         && children[0].entryStart == entryStart && children[0].entryEnd == entryEnd);
        }

        bool hasTopLevelChild() const
        {
            for (auto & c: children) {
                if (c.isTopLevel() || c.hasTopLevelChild())
                    return true;
            }

            return false;
        }

        size_t childTopNestingLevel() const
        {
            if (!isTopLevel())
                return 0;
            size_t result = 0;
            for (auto & c: children) {
                result = std::max(result, 1 + c.childTopNestingLevel());
            }
            return result;
        }

        bool childrenOverlap() const
        {
            for (auto & ch: children) {
                if (ch.children.size() > 0)
                    return true;
            }
            return false;
        }

        std::vector<NestedEntry> children;
    };

    auto dumpHex = [&] (const std::string & prefix, size_t start, size_t len, const std::string & tag,
                        const std::span<const NestedEntry> & children = {})
    {
        uint32_t lastVal = MAX_LIMIT;

        struct Child {
            size_t start;
            size_t end;
            size_t entry;
            size_t level;
            //std::string color;
        };

        std::multimap<size_t, Child> childStarts;
        std::multimap<size_t, Child> childEnds;

        size_t maxLevel = 0;
        std::function<void (const NestedEntry &, size_t)> doChild = [&] (const NestedEntry & entry, size_t level)
        {
            maxLevel = std::max(level, maxLevel);
            Child child;
            child.start = entry.entryStart;
            child.end = entry.entryEnd;
            child.entry = entry.entryIndex;
            child.level = level;

            childStarts.emplace(child.start, child);
            childEnds.emplace(child.end, child);

            for (auto & c: entry.children) {
                doChild(c, level + 1);
            }
        };

        for (auto & c: children) {
            doChild(c, 0);
        }

        bool firstLine = true;
        std::set<size_t> activeChildren;

        // Highlight array elements
        int arrayElementLength = -1;
        int arrayNumElements = 0;
        if (children.size() == 1) {
            auto & entry = stats.entries.at(children[0].entryIndex);
            if (entry.arrayLen != MappingContext::SCALAR) {
                size_t len = entry.len;
                if (len % entry.arrayLen == 0) {
                    arrayElementLength = len / entry.arrayLen;
                    arrayNumElements = entry.arrayLen;
                }
            }
        }

        for (size_t lineStart = start & ~15;  lineStart < start + len;  lineStart += 16, firstLine = false) {
            stream << prefix << format("%s%08llx%s ", ansi_str_bright_blue(), lineStart, ansi_str_reset());
            std::set<size_t> childrenStarted, childrenEnded;
            for (size_t pos = lineStart;  pos < lineStart + 16;  ++pos) {
                if (pos < start || pos >= start + len)
                    stream << "   ";
                else {
                    int elementNumber = 0;
                    bool betweenArrayElements = false;
                    if (arrayElementLength > 0) {
                        //int byteNumberInElement = (pos - start) % arrayElementLength;
                        elementNumber = (pos - start) / arrayElementLength;
                        if (elementNumber % 2 == 0)
                            stream << bright_white;
                        betweenArrayElements = (pos - start + 1) % arrayElementLength == 0
                             && elementNumber != arrayNumElements - 1;
                    }

                    uint32_t val = (uint8_t)data[pos];
                    if (val == 0)
                        stream << (elementNumber % 2 == 0 ? bright_red : red);
                    else if (val == lastVal || (pos < start + len - 1 && (uint8_t)data[pos + 1] == val))
                        stream << (elementNumber % 2 == 0 ? bright_yellow : yellow);

                    stream << format("%02x", val);
                    stream << reset;
                    if (betweenArrayElements)
                        stream << magenta << "," << reset;
                    else stream << " ";
 
                    lastVal = val;
                }
                if (pos % 4 == 3)
                    stream << " ";
                if (pos % 8 == 7)
                    stream << " ";

                for (auto it = childStarts.lower_bound(pos), end = childStarts.upper_bound(pos);  it != end;  ++it) {
                    childrenStarted.insert(it->second.entry);
                }

                for (auto it = childEnds.lower_bound(pos + 1), end = childEnds.upper_bound(pos + 1);  it != end;  ++it) {
                    childrenEnded.insert(it->second.entry);
                }

            }
            if (firstLine && !tag.empty())
                stream << " " << tag;

            for (auto & ch: childrenStarted) {
                activeChildren.insert(ch);
                auto & entry = stats.entries.at(ch);
                if (entry.info) {
                    if (entry.info->kind == ValueKind::STRUCTURE) {
                        stream << "structure: " << entry.info->printJsonString(entry.start + entry.offset) << endl;
                    }
                    else {
                        stream << "non-structure: " << entry.info->printJsonString(entry.start + entry.offset) << endl;
                    }
                } else {
                    //stream << "type " << entry.typeName() << " has no value description" << endl;
                    stream << "(opaque)" << endl;
                }
            }

            for (auto c: activeChildren) {
                //bool started = childrenStarted.count(c);
                //bool ended = childrenEnded.count(c);

                std::string entryStr;
                auto & entry = stats.entries.at(c);
                entryStr += string(ansi_str_bright_blue()) + "[" + format("%03x", (int)entry.offset % 4096);
                entryStr += "-";
                entryStr += format("%03x]", (int)(entry.offset + entry.len) % 4096) + ansi_str_reset();
                entryStr += " " + entry.shortColoredString();

                stream << " " << entryStr;
            }

            // Print which array element(s) are in this row
            int elementNumberAtStart = 0;
            int elementNumberAtEnd = 0;
            if (arrayElementLength > 0) {
                //int byteNumberInElement = (pos - start) % arrayElementLength;
                elementNumberAtStart = std::max<ssize_t>(0, lineStart - start) / arrayElementLength;
                elementNumberAtEnd = std::min(lineStart - start + 15, len - 1) / arrayElementLength;
                stream << magenta << " [" << elementNumberAtStart;
                if (elementNumberAtEnd != elementNumberAtStart)
                stream << "-" << elementNumberAtEnd;
                stream << "]" << reset;
            }

            for (auto & ch: childrenEnded)
                activeChildren.erase(ch);
            stream << endl;
        }
    };

    std::function<std::pair<NestedEntry, size_t> (size_t index)> doChildIndexes = [&] (size_t index) -> std::pair<NestedEntry, size_t>
    {
        NestedEntry result;
        result.entryIndex = index;
        if (index >= stats.entries.size())
            return std::make_pair(std::move(result), index);

        auto & parentEntry = stats.entries[index];
        size_t parentStart = parentEntry.offset;
        size_t parentEnd = parentStart + parentEntry.len;

        result.entryStart = parentStart;
        result.entryEnd = parentEnd;

        for (++index; index < stats.entries.size();  /* no inc */) {
            auto & childEntry = stats.entries[index];
            size_t childStart = childEntry.offset;
            size_t childEnd = childStart + childEntry.len;
            if (childStart == childEnd) {
                ++index;
                continue;
            }
            if (childEnd > parentEnd)
                break;

            auto && [childResult, newIndex] = doChildIndexes(index);
            ExcAssert(newIndex > index);
            result.children.emplace_back(std::move(childResult));
            index = newIndex;
        }

        return std::make_pair(std::move(result), index);
    };

    NestedEntry root = doChildIndexes(0).first;
    size_t numTopLevels = root.childTopNestingLevel();
    size_t bytesDone = 0;

    std::function<void (const NestedEntry &, size_t, bool)> dumpLevel = [&] (const NestedEntry & nesting, size_t level, bool firstChild)
    {
        auto & entry = stats.entries[nesting.entryIndex];

        std::string prefix(level, '|');  // continuation of levels above
        if (firstChild) prefix += '\\';
        else prefix += '\\';
        prefix.resize(numTopLevels + 2, ' ');
        std::string pathEntry = entry.pathStr();
        std::string typeName = entry.typeName();
        stream << prefix << bold << bright_blue << format("%8zx-%-6zx", entry.offset, (entry.offset + entry.len)) << " " << cyan << pathEntry
               << reset << ": " << bold << green << typeName << reset;
        //cerr << " with " << nesting.children.size() << " children" << endl;
        if (entry.len > 64) {
            stream << std::string(std::max<int64_t>(0, 80 - pathEntry.size() - typeName.size()), ' ')
                   << entry.colorCompressibility();
        }
        stream << endl;

        if (nesting.hasTopLevelChild()) {
            bool firstChild = true;
            for (auto & c: nesting.children) {
                dumpLevel(c, level + 1, firstChild);
                firstChild = false;
            }
            return;
        }

        if (entry.offset > bytesDone) {
            // There is empty space
            dumpHex(prefix, bytesDone, entry.offset - bytesDone, ansi_str_bg_red() + string("dead space") + ansi_str_reset());
            bytesDone = entry.offset;
        }

        prefix[level] = '|';

        if (nesting.children.size() > 0) {
           dumpHex(prefix, nesting.entryStart, nesting.len(), "" /* tag */, nesting.children);
        }
        else {
            dumpHex(prefix, nesting.entryStart, nesting.len(),
                    "" /* tag */, {&nesting, 1});
        }
        bytesDone = nesting.entryEnd;
    };

    dumpLevel(root, 0, true);
}

// Initialize the context, with a maximum mapped size.  If we attempt to freeze objects
// that don't fit within that size, we will get an exception.
MappingContext::MappingContext(size_t maxSize)
    : sharedState_(new SharedState()), localState_(new LocalState())
{
    // Make sure it's page aligned so object alignment works
    void * mem = ::aligned_alloc(4096, maxSize);
    if (mem == nullptr)
        MLDB_THROW_BAD_ALLOC();

    // Use a shared pointer so it gets freed automatically once the object is destroyed
    sharedState_->mem_.reset((std::byte *)mem, [] (std::byte * mem) { free(mem); });
    
    // Nothing allocated yet
    sharedState_->offset_ = 0;

    // Arena length
    sharedState_->len_ = maxSize;

    // Mapping and statstics
    sharedState_->stats.reset(new Stats());
}

// Constructor for a child context with the given options set
MappingContext::MappingContext(MappingContext * parent, std::unordered_map<const MappingOptionBase *, std::any> options)
    : localState_(new LocalState())
{
    if (!parent)
        MLDB_THROW_LOGIC_ERROR("MappingContext with null parent");
    this->sharedState_ = parent->sharedState_;
    localState_->parent = parent;
    localState_->startOffset = sharedState_->offset_;
    localState_->options = std::move(options);
}

// Constructor for a child context with the given addition to the path, and the given options set
MappingContext::MappingContext(MappingContext * parent, std::string name, const std::type_info * type,
                               const std::shared_ptr<const ValueDescription> info, size_t arrayLen, Directness directness,
                               std::unordered_map<const MappingOptionBase *, std::any> options)
    : localState_(new LocalState())
{
    if (!parent)
        MLDB_THROW_LOGIC_ERROR("MappingContext with null parent");
    this->sharedState_ = parent->sharedState_;
    localState_->parent = parent;
    localState_->startOffset = sharedState_->offset_;
    localState_->options = std::move(options);
    localState_->name = name;
    localState_->type = type;
    localState_->info = info;
    localState_->directness = directness;
    localState_->arrayLen = arrayLen;
}

MappingContext::~MappingContext()
{
    if (localState_->parent == nullptr && sharedState_.use_count() == 1) {
        printStats(*sharedState_->stats, getOption(DUMP_MEMORY_MAP), getOption(DUMP_TYPE_STATS));
    }
}

void MappingContext::childExitScope(const LocalState & state)
{
    if (!state.type)
        return;
    Stats::Entry entry;
    entry.start = sharedState_->mem_.get();
    entry.offset = state.startOffset;
    entry.len = sharedState_->offset_ - state.startOffset;
    entry.type = state.type;
    entry.info = state.info;
    entry.directness = state.directness;
    entry.path = path();
    entry.arrayLen = state.arrayLen;
    entry.path.push_back(state.name);

    Stats & stats = *sharedState_->stats;
    stats.entries.push_back(entry);
}

void MappingContext::setOption(MappingOptionBase::Value value)
{
    if (!value.option)
        MLDB_THROW_LOGIC_ERROR("MappingContext::setOption(): null option");
    localState_->options[value.option] = value.value;
}

void MappingContext::unsetOption(const MappingOptionBase & option)
{
    localState_->options.erase(&option);
}

const std::any & MappingContext::getOption(const MappingOptionBase & option) const
{
    auto it = localState_->options.find(&option);
    if (it != localState_->options.end()) return it->second;
    if (localState_->parent) return localState_->parent->getOption(option);
    return option.defValue();
}

std::vector<std::string> MappingContext::path() const
{
    std::vector<std::string> result;
    if (localState_->parent)
        result = localState_->parent->path();
    if (!localState_->name.empty())
        result.push_back(localState_->name);
    return result;
}

// Return the current offset in the array.  Mostly used for debugging or calculting
// the storage efficiency.
size_t MappingContext::getOffset() const
{
    return sharedState_->offset_;
}

// Return the offset in the arena of the given object.  Throws if the object is not
// in the arena.
size_t MappingContext::getOffset(const void * obj) const
{
    assertInArena(obj);
    return (const std::byte *)obj - getMemory(0);
}

// Return the memory at the given offset in the array.  Mostly used for
// debugging or calculating the storage efficiency.
const std::byte * MappingContext::getMemory(size_t offset) const
{
    return sharedState_->mem_.get() + offset;
}

// Allocate an unaligned block of the given size
std::byte * MappingContext::malloc(size_t size)
{
    using namespace std;
    //cerr << "malloc of size " << size << " at offset " << hex << sharedState_->offset_ << dec << endl;
    if (sharedState_->offset_ + size > sharedState_->len_)
        MLDB_THROW_BAD_ALLOC();
    std::byte * result = sharedState_->mem_.get() + sharedState_->offset_;
    sharedState_->offset_ += size;
    return result;
}

// Assert that the given object is insize the arena.  Since the pointers
// don't work when outside, this ensures that we don't make any mistakes
// by pointing to objects allocated in a different arena or on the stack.
//
// Note that we allow things to be at the end of the arena, as zero-length
// objects need to be able to be somewhere.
void MappingContext::assertInArena(const void * obj) const
{
    auto o2 = (const std::byte *)obj;
    if (o2 < sharedState_->mem_.get() || o2 > sharedState_->mem_.get() + sharedState_->offset_) {
        using namespace std;
        cerr << "not in arena: address " << obj << endl;
        cerr << "arena start " << (void *)sharedState_->mem_.get() << " end " << (void *)(sharedState_->mem_.get() + sharedState_->offset_) << endl;
        cerr << "arena offset " << sharedState_->offset_ << " effective offset " << o2 - sharedState_->mem_.get() << endl;
    }
    ExcAssert(o2 >= sharedState_->mem_.get() && o2 <= sharedState_->mem_.get() + sharedState_->offset_);
}

void MappingContext::assertWritable(void * obj)
{
    // TODO: keep track of what's finalized vs still mutable
    assertInArena(obj);
}

// For debugging, tag with a string, which will be visible inside the mapped file
// to understand its content.  This won't affect the functionality of the file
// as the string will be inserted into "lost space" in the file.
//void MappingContext::tag(const char * str)
//{
//    if (getOption(DEBUG))
//        array(str, strlen(str));
//}

template<typename Ofs>
const std::byte * getMappedPtrBase(const Ofs * ofs, size_t align)
{
    size_t s = reinterpret_cast<size_t>(ofs + 1);
    s = (s + align - 1) / align * align;
    ExcAssert(s % align == 0);
    return reinterpret_cast<const std::byte *>(s);
}

template<typename Out, typename In>
constexpr bool valueFitsIn(In val)
{
    Out res;
    return !__builtin_add_overflow (val, 0, &res);
}

// Set the pointer to the given value, checking it's in range first
template<typename Ofs>
void setMappedPtr(Ofs * ofsPtr, const void * ptr, size_t align)
{
    const std::byte * base = getMappedPtrBase(ofsPtr, align);
    size_t sb = reinterpret_cast<size_t>(base);
    size_t sp = reinterpret_cast<size_t>(ptr);

    if (sp < sb) {
        MLDB_THROW_RUNTIME_ERROR("attempt to use unsigned MappedPtr to point earlier");
    }

    ssize_t ofs = sp - sb;
    ExcAssert(ofs % align == 0);
    ofs /= align;

    if (!valueFitsIn<Ofs>(ofs)) {
        MLDB_THROW_RUNTIME_ERROR("pointer is too big for available bits");
    }
    *ofsPtr = ofs;

#if 0
    if (this->get() != ptr) {
        cerr << "type " << demangleTypeName(typeid(T).name()) << endl;
        cerr << "alignment " << alignof(T) << endl;
        cerr << "size " << sizeof(T) << endl;
        cerr << "sb = " << hex << sb << dec << endl;
        cerr << "sp = " << hex << sp << dec << endl;
        cerr << "this->base() = " << this->getBase() << endl;
        cerr << "this->get() = " << this->get() << endl;
        cerr << "ptr = " << ptr << endl;
    }
    ExcAssert(this->get() == ptr);
#endif
}

void setMappedPtr(uint32_t * ofs, const void * ptr, size_t align)
{
    return setMappedPtr<uint32_t>(ofs, ptr, align);

}

void setMappedPtr(int32_t * ofs, const void * ptr, size_t align)
{
    return setMappedPtr<int32_t>(ofs, ptr, align);

}

void setMappedPtr(uint64_t * ofs, const void * ptr, size_t align)
{
    return setMappedPtr<uint64_t>(ofs, ptr, align);

}

void setMappedPtr(int64_t * ofs, const void * ptr, size_t align)
{
    return setMappedPtr<int64_t>(ofs, ptr, align);

}


// We freeze a string into a pointer by simply copying it in and returning the pointer.  Note that
// we don't store null terminators; it is assumed that the length of the string is known from
// elsewhere.
void freeze(MappingContext & context, MappedPtr<char> & output, const std::string & input)
{
    char * p = (char *)context.malloc(input.length());
    std::copy(input.data(), input.data() + input.length(), p);
    freeze(context, output, p);
}

// Freeze the data and then the length
void freeze(MappingContext & context, MappedString & output, const std::string & input)
{
    ExcAssert(input.length() < std::numeric_limits<uint32_t>::max());
    freeze(context, output.data_, input);
    freeze(context, output.length_, input.length());
}

} // namespace MLDB
