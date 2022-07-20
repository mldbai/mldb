/* prefix_trie.h                                               -*- C++ -*-
   Jeremy Barnes, 6 November 2015
   Copyright (c) 2015 mldb.ai inc.  All rights reserved.

   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
*/

#pragma once

#include <string>
#include <vector>
#include <cstddef>
#include <optional>
#include "mmap.h"
#include "mapped_int_table.h"
#include "mldb/arch/endian.h"
#include "mldb/arch/vm.h"

namespace MLDB {

enum PrefixTrieNodeType: uint8_t {
    EMPTY,
    DENSE,
    TAIL,
    SPARSE,
    LEAF,
};

#define MLDB_FOR_EACH_PREFIX_TRIE_NODE_TYPE(op, ...) \
    op(EMPTY, EmptyNode, __VA_ARGS__) \
    op(DENSE, DenseNode, __VA_ARGS__) \
    op(TAIL,  TailNode,  __VA_ARGS__) \

/*
    op(SPARSE, SparseNode, __VA_ARGS__) \
    op(LEAF, LeafNode, __VA_ARGS__) \
*/

#define MLDB_DO_PREFIX_TRIE_TYPE_SWITCH(val, tp, node, typedNode, ...) \
case PrefixTrieNodeType::val: { const auto & typedNode = (node).template atOffset<tp>(0); __VA_ARGS__; break; } \


#define MLDB_PREFIX_TRIE_TYPE_SWITCH(node, typedNode, ...) \
switch ((PrefixTrieNodeType)(node).type_) { \
MLDB_FOR_EACH_PREFIX_TRIE_NODE_TYPE(MLDB_DO_PREFIX_TRIE_TYPE_SWITCH, node, typedNode, __VA_ARGS__) \
default: MLDB_THROW_LOGIC_ERROR(); \
} 

template<typename Node>
inline void get_trie_node_val(const Node * n, uint16_t & val)
{
    val = n->getVal();
}

template<typename Node>
inline void set_trie_node_val(Node * n, uint16_t val)
{
    n->setVal(val);
}

inline size_t get_trie_node_num_bytes(uint16_t val)
{
    return 2;
}

template<typename N>
inline N * aligned_to(const void * p)
{
    auto a = alignof(N);
    intptr_t i = reinterpret_cast<intptr_t>(p);
    auto aoffset = i % a; // current offset from alignment
    auto padding = (a - aoffset) % a;  // how many we need to skip to get the right alignment
    if (padding)
        i += padding;
    return reinterpret_cast<N *>(i);
}

// Allocate without a node
template<typename Node, typename Val>
Node & alloc_trie_node(MappingContext & context, std::string_view prefix, size_t extraBytes, bool hasVal)
{
    //static_assert(alignof(Node) == 1, "Only packed / byte aligned trie nodes can be allocated");
    Node & res = context.alloc_field<Node>(std::string(prefix) + ".node");
    if (extraBytes)
        context.malloc(extraBytes);
    if (hasVal)
        context.alloc_field<Val>(std::string(prefix) + ".val");
    return res;
}

struct PrefixTrieNodeHeader {
    struct {
        uint8_t type_:3 = -1;
        uint8_t hasVal_:1 = 0;
        uint8_t data_:4 = 0;
    };

    template<typename N> const N & atOffset(size_t n) const
    {
        auto p = reinterpret_cast<const std::byte *>(this);
        p += n;
        return *reinterpret_cast<const N *>(p);
    }
} MLDB_PACKED;


template<typename Ch, typename Val, typename Base>
struct PrefixTrieImpl: Base {
private:
    using Base::data;
    using Base::dataLength;
    using NodeHeader = PrefixTrieNodeHeader;

    template<typename T> const T * atOffset(uint32_t offset) const
    {
        size_t endOffset;
        if (__builtin_add_overflow(offset, sizeof(T), &endOffset))
            MLDB_THROW_RANGE_ERROR("Overflow calculating end of range");
        ExcAssertLessEqual(endOffset, dataLength());
        return reinterpret_cast<const T *>(data() + offset);
    }

public:
    Val get(const Ch * p, Val def = Val()) const
    {
        return this->getImpl(*atOffset<NodeHeader>(0), p, p + std::char_traits<Ch>::length(p), def);
    }

    template<typename Seq>
    Val get(Seq&&seq, Val def = Val()) const
    {
        return atOffset<Node>(0)->getImpl(seq.begin(), seq.end(), def);
    }

    size_t size() const
    {
        return this->size(*atOffset<NodeHeader>(0));
    }

    void dump(std::ostream & stream, size_t indent = 0) const
    {
        dumpImpl(*atOffset<NodeHeader>(0), stream, indent);
    }

//private:
    template<typename Node>
    static Val getVal(const Node & node)
    {
        auto doNode = [&] (const auto & typedNode) { return typedNode.getVal(); };
        MLDB_PREFIX_TRIE_TYPE_SWITCH(node, typedNode, return doNode(typedNode));
    }

    template<typename Node, typename It>
    static Val getImpl(const Node & node, It first, It last, Val def)
    {
        if (first == last) {
            using namespace std;
            //cerr << "getImpl " << std::string(first, last) << "hasVal_ " << (int)node.hasVal_ << endl;
            if (node.hasVal_)
                return getVal(node);
            else return def;
        }

        auto doNode = [&] (const auto & typedNode) { return typedNode.getChild(first, last, def); };
        MLDB_PREFIX_TRIE_TYPE_SWITCH(node, typedNode, return doNode(typedNode));
    }

    template<typename Node>
    static Val size(const Node & node)
    {
        //using namespace std;
        //cerr << "size of " << (int)node.type_ << endl;
        MLDB_PREFIX_TRIE_TYPE_SWITCH(node, typedNode, return typedNode.size(); );
    }

    static void dumpImpl(const NodeHeader & node, std::ostream & stream, size_t indent = 0)
    {
        MLDB_PREFIX_TRIE_TYPE_SWITCH(node, typedNode, typedNode.dumpImpl(stream, indent);  return);
    }

    // Empty node, no sub-keys, may have a value
    struct EmptyNode: public NodeHeader {

        template<typename It>
        Val getChild(It first, It last, Val def) const
        {
            ExcAssert(first != last);
            return def;
        }

        size_t size() const { return hasVal_; }

        Val getVal() const
        {
            ExcAssert(hasVal_);
            return val[0];
        }

        void setVal(Val newVal)
        {
            ExcAssert(hasVal_);
            val[0] = std::move(newVal);
        }

        void dumpImpl(std::ostream & stream, size_t indent = 0) const
        {
            std::string i(indent, ' ');
            stream << i << "EMPTY " << " ";
            if (hasVal_)
                stream << " VAL(" << getVal() << ")";
            else
                stream << " NO VAL";
            stream << std::endl;
        }

        LittleEndian<Val> val[0];

        static auto & alloc(MappingContext & context, std::string_view prefix, std::optional<Val> val)
        {
            auto & node = alloc_trie_node<EmptyNode, Val>(context, prefix, 0 /* extra */, val.has_value());
            node.type_ = PrefixTrieNodeType::EMPTY;
            node.hasVal_ = val.has_value();
            node.data_ = 0;
            if (val) {
                set_trie_node_val(&node, *val);
            }
            return node;
        }
    };

    // Dense node, with (up to) 256 pointers, one per character
    struct DenseNode: public NodeHeader {
        using NodeHeader::atOffset;
        template<typename It>
        Val getChild(It first, It last, Val def) const
        {
            ExcAssert(first != last);
            uint32_t c = *first++;
            if (c >= offsets.size())
                return def;
            auto ofs = offsets.at(c);
            if (ofs == 0)
                return def;
            //if (hasVal_)
            //    ofs += valNumBytes();
            return PrefixTrieImpl::getImpl(NodeHeader::template atOffset<NodeHeader>(ofs), first, last, def);
        }

        size_t size() const
        {
            size_t result = hasVal_;

            for (size_t i = 0;  i < offsets.size();  ++i) {
                auto ofs = offsets.at(i);
                if (ofs == 0)
                    continue;
                result += PrefixTrieImpl::size(NodeHeader::template atOffset<NodeHeader>(ofs));
            }
            return result;
        }

        size_t valNumBytes() const
        {
            return sizeof(Val);
        }

        Val getVal() const
        {
            ExcAssert(hasVal_);
            return val[0];
        }

        void setVal(Val newVal)
        {
            ExcAssert(hasVal_);
            val[0] = std::move(newVal);
        }

        void dumpImpl(std::ostream & stream, size_t indent = 0) const
        {
            using namespace std;
            std::string ind(indent, ' ');
            stream << ind << "DENSE";
            if (hasVal_)
                stream << " VAL(" << getVal() << ")";
            else
                stream << " NO VAL";
            stream << std::endl;

            //using namespace std;
            //cerr << "doing " << offsets.size() << endl;
            for (uint32_t i = 0;  i < offsets.size();  ++i) {
                auto ofs = offsets.at(i);
                if (ofs == 0)
                    continue;
                //if (hasVal_)
                //    ofs += valNumBytes();
                //cerr << "i " << i << " ofs " << ofs << endl;
                stream << ind << "  '" << char(i) << "':"
                       << " ofs " << ofs
                       << std::endl;
                PrefixTrieImpl::dumpImpl(NodeHeader::template atOffset<NodeHeader>(ofs), stream, indent + 4);
            }
        }

        RawMappedIntTable offsets;
        LittleEndian<Val> val[0];

        static auto & alloc(MappingContext & context, std::string_view prefix, std::optional<Val> val)
        {
            auto & node = alloc_trie_node<DenseNode, Val>(context, prefix, 0 /* extra */, val.has_value());
            node.type_ = PrefixTrieNodeType::DENSE;
            node.hasVal_ = val.has_value();
            node.data_ = 0;
            if (val) {
                set_trie_node_val(&node, *val);
            }
            return node;
        }
    };

    // Dense node, with (up to) 256 pointers, one per character
    struct TailNode: public NodeHeader {
        using NodeHeader::atOffset;
        template<typename It>
        Val getChild(It first, It last, Val def) const
        {
            for (size_t i = 0;  i < len_;  ++i, ++first) {
                if (first == last || tailChars_[i] != *first)
                    return def;
            }
            return getVal();
        }

        size_t size() const
        {
            return 1;
        }

        size_t valNumBytes() const
        {
            return sizeof(Val);
        }

        Val getVal() const
        {
            return *getValPtr();
        }

        void setVal(Val newVal)
        {
            *getValPtr() = newVal;
        }

        void dumpImpl(std::ostream & stream, size_t indent = 0) const
        {
            std::string i(indent, ' ');
            stream << i << "TAIL " << " ";
            stream << getSuffix() << " ";
            stream << " VAL(" << getVal() << ")";
            stream << std::endl;
        }

        uint8_t len_;
        char tailChars_[0];

        Val * getValPtr() const
        {
            return aligned_to<Val>(((const std::byte *)this) + sizeof(*this) + len_);
        }

        std::string_view getSuffix() const
        {
            return { tailChars_, len_ };
        }

        static auto & alloc(MappingContext & context, std::string_view prefix, std::string_view key, Val val)
        {
            size_t n = key.size() - prefix.size();
            auto & node = alloc_trie_node<TailNode, Val>(context, prefix, n /* extra */, true /* has value */);
            node.type_ = PrefixTrieNodeType::TAIL;
            node.hasVal_ = true;
            node.data_ = 0;
            ExcAssertLess(n, 256);
            node.len_ = n;
            std::copy(key.data() + prefix.size(), key.data() + key.size(), node.tailChars_);
            if (val) {
                set_trie_node_val(&node, val);
            }
            return node;
        }

    };

    struct SparseNode: public NodeHeader {
    } MLDB_PACKED;

    struct LeafNode: public NodeHeader {
    } MLDB_PACKED;

    struct Node: public NodeHeader {

    } MLDB_PACKED;
};

struct MemPrefixTrieBase {

    std::vector<std::byte> mem;
    const std::byte * data() const { return mem.data(); }
    size_t dataLength() const { return mem.size(); }

    size_t memUsageIndirect(const MemUsageOptions & opt) const
    {
        return MLDB::memUsageIndirect(mem, opt);
    }
};

struct CharPrefixTrie: public PrefixTrieImpl<char8_t, uint16_t, MemPrefixTrieBase> {};

// Insert the entries between first and last sorted (iterators over pair<stringlike, value>)
// into a newly constructed node on output, ignoring the prefixLen first characters in each
// string key.  Returns the offset of the node in the mapping context's arena.
template<typename It>
inline size_t construct_trie_node(MappingContext & context, std::string_view prefix, It first, It last)
{
    using namespace std;

    size_t prefixLen = prefix.size();
    //cerr << "prefix.size() = " << prefix.size() << endl;

    size_t n = std::distance(first, last);

    using Val = std::decay_t<decltype(first->second)>;

    if (n == 0) {
        auto & node = CharPrefixTrie::EmptyNode::alloc(context, prefix, nullopt);
        return context.getOffset(&node);
    }
    else if (n == 1 && first->first.size() == prefix.size()) {
        // Just a value
        auto & node = CharPrefixTrie::EmptyNode::alloc(context, prefix, first->second);
        return context.getOffset(&node);
    }
    else if (n == 1) {
        auto & node = CharPrefixTrie::TailNode::alloc(context, prefix, first->first, first->second);
        return context.getOffset(&node);
    }

    size_t minLen = MAX_LIMIT;
    size_t maxLen = 0;
    size_t totalLen = 0;

    for (auto it = first;  it != last;  ++it) {
        const auto & [key, val] = *it;
        ExcAssertGreaterEqual(key.size(), prefixLen);
        auto suffixLen = key.size() - prefixLen;
        minLen = std::min(minLen, suffixLen);
        maxLen = std::max(maxLen, suffixLen);
        totalLen += suffixLen;
    }

    std::optional<Val> val;
    if (first->first.size() == prefix.size()) {
        val = first->second;
        ++first;
    }
    auto & node = CharPrefixTrie::DenseNode::alloc(context, prefix, val);
    size_t startOffset = context.getOffset(&node);

    std::vector<uint32_t> offsets;
    offsets.reserve(256);

    auto getCh = [&] () -> uint32_t { return first->first.at(prefixLen); };

    while (first != last) {
        //cerr << "doing " << first->first << " with prefix " << prefix << endl;
        ExcAssert(first->first.find(prefix) == 0);
        if (first->first.size() == prefix.size())
            continue;  // skip the value associated with the empty key
        auto it = first;
        auto c = getCh();
        std::string_view newPrefix(first->first.data(),
                                   prefix.size() + 1);
        //cerr << "str = " << first->first << " prefix " << prefix << " newPrefix = " << newPrefix << endl;

        ++first;
        while (first != last && getCh() == c)
            ++first;
        //cerr << "c = " << (int)c << endl;
        size_t ofs = construct_trie_node(context, newPrefix, it, first) - startOffset;
        ExcAssertGreater(ofs, 0);
        //cerr << "done constructing " << std::distance(it, first) << " with prefix " << newPrefix << " at offset " << ofs << endl;
        if (c >= offsets.size())
            offsets.resize(c + 1);
        offsets[c] = ofs;
    }

    //cerr << "freezing " << offsets.size() << " offsets for " << prefix << endl;

    freeze(context, node.offsets, offsets);

    //cerr << "done construction " << prefix << endl;
    return startOffset;
    //MLDB_THROW_UNIMPLEMENTED();
}

inline CharPrefixTrie construct_trie(const std::map<std::string, uint16_t> & m)
{
    CharPrefixTrie result;
    size_t totalLength = 256;
    for (auto & [key, val]: m) {
        totalLength += 24 + key.size() + sizeof(m);
    }

    MappingContext context(roundUpToPageSize(totalLength)
//                           , DUMP_MEMORY_MAP=true
                           );
    construct_trie_node(context, {}, m.begin(), m.end());
    using namespace std;
    cerr << "allocated " << context.getOffset() << " bytes for " << m.size() << " entries" << endl;
    result.mem = { context.getMemory(), context.getMemory() + context.getOffset() };
    return result;
}

} // namespace MLDB
