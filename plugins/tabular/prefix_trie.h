/* prefix_trie.h                                               -*- C++ -*-
   Jeremy Barnes, 6 November 2015
   Copyright (c) 2015 mldb.ai inc.  All rights reserved.

   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
*/

#pragma once

#include <string>
#include <vector>
#include <cstddef>
#include "mmap.h"
#include "mapped_int_table.h"
#include "mldb/arch/endian.h"
#include "mldb/arch/vm.h"

namespace MLDB {

enum PrefixTrieNodeType: uint8_t {
    EMPTY,
    DENSE,
    SPARSE,
    LEAF,
};

#define MLDB_FOR_EACH_PREFIX_TRIE_NODE_TYPE(op, ...) \
    op(EMPTY, EmptyNode, __VA_ARGS__) \
    op(DENSE, DenseNode, __VA_ARGS__) \

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
            cerr << "getImpl " << std::string(first, last) << "hasVal_ " << (int)node.hasVal_ << endl;
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
            stream << i << "EMPTY ";
            if (hasVal_)
                stream << " VAL(" << getVal() << ")";
            else
                stream << " NO VAL";
        }

        LittleEndian<Val> val[0];
    } MLDB_PACKED;

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
            if (hasVal_)
                ofs += valNumBytes();
            return PrefixTrieImpl::getImpl(NodeHeader::template atOffset<NodeHeader>(ofs), first, last, def);
        }

        size_t size() const
        {
            size_t result = 0;
            size_t nb = valNumBytes();

            for (size_t i = 0;  i < offsets.size();  ++i) {
                auto ofs = offsets.at(i);
                if (ofs == 0)
                    continue;
                result += PrefixTrieImpl::size(NodeHeader::template atOffset<NodeHeader>(ofs + nb));
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
            std::string i(indent, ' ');
            stream << i << "DENSE";
            if (hasVal_)
                stream << " VAL(" << getVal() << ")";
            else
                stream << " NO VAL";

            for (uint32_t i = 0;  i < offsets.size();  ++i) {
                auto ofs = offsets.at(i);
                if (ofs == 0)
                    continue;
                if (hasVal_)
                    ofs += valNumBytes();
                stream << i << "    " << char(i) << ":" << std::endl;
                PrefixTrieImpl::dumpImpl(NodeHeader::template atOffset<NodeHeader>(ofs), stream, indent + 4);
            }
        }

        RawMappedIntTable offsets;
        LittleEndian<Val> val[0];
    } MLDB_PACKED;

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

// Allocate without a node
template<typename Node>
Node & alloc_trie_node(MappingContext & context, std::string_view prefix, size_t valBytes)
{
    static_assert(alignof(Node) == 1, "Only packed / byte aligned trie nodes can be allocated");
    return context.alloc_field<Node>(std::string(prefix));
}

// Insert the entries between first and last sorted (iterators over pair<stringlike, value>)
// into a newly constructed node on output, ignoring the prefixLen first characters in each
// string key.
template<typename It>
inline void construct_trie_node(MappingContext & context, std::string_view prefix, It first, It last)
{
    size_t prefixLen = prefix.size();
    cerr << "prefix.size() = " << prefix.size() << endl;

    size_t n = std::distance(first, last);

    if (n == 0) {
        auto & node = alloc_trie_node<CharPrefixTrie::EmptyNode>(context, prefix, 0 /* valBytes */);
        node.type_ = PrefixTrieNodeType::EMPTY;
        node.hasVal_ = 0;
        node.data_ = 0;
        return;
    }
    if (n == 1 && first->first.empty()) {
        // Just a value
        size_t valBytes = get_trie_node_num_bytes(first->second);
        auto & node = alloc_trie_node<CharPrefixTrie::EmptyNode>(context, prefix, valBytes);
        node.type_ = PrefixTrieNodeType::EMPTY;
        node.hasVal_ = 1;
        node.data_ = 0;
        set_trie_node_val(&node, first->second);
        return;
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

    size_t startOffset = context.getOffset();

    bool hasVal = first->first.empty();
    size_t valBytes = 0;
    if (hasVal) {
        valBytes = get_trie_node_num_bytes(first->second);
    }
    auto & node = alloc_trie_node<CharPrefixTrie::DenseNode>(context, prefix, valBytes);
    node.type_ = PrefixTrieNodeType::DENSE;
    node.hasVal_ = hasVal;
    node.data_ = 0;
    if (hasVal) {
        set_trie_node_val(&node, first->second);
        ++first;
    }

    std::vector<uint32_t> offsets;
    offsets.reserve(256);

    auto getCh = [&] () -> uint32_t { return first->first.at(prefixLen); };

    for (; first != last;  ++first) {
        auto it = first;
        auto c = getCh();
        while (first != last && getCh() == c)
            ++first;
        size_t ofs = context.getOffset() - startOffset;
        ExcAssertGreater(ofs, 0);
        std::string_view newPrefix(first->first.data() + prefix.size() + 1,
                                   first->first.size() - prefix.size() - 1);
        construct_trie_node(context, newPrefix, it, first);
        offsets.resize(c + 1);
        offsets[c] = ofs;
    }

    freeze(context, node.offsets, offsets);

    MLDB_THROW_UNIMPLEMENTED();
}

inline CharPrefixTrie construct_trie(const std::map<std::string, uint16_t> & m)
{
    CharPrefixTrie result;
    size_t totalLength = 256;
    for (auto & [key, val]: m) {
        totalLength += sizeof(PrefixTrieNodeHeader) + key.size() + sizeof(m);
    }

    MappingContext context(roundUpToPageSize(totalLength));
    construct_trie_node(context, {}, m.begin(), m.end());
    result.mem = { context.getMemory(), context.getMemory() + context.getOffset() };
    return result;
}

} // namespace MLDB
