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
#include "string_table_iterator.h"
#include "mldb/arch/endian.h"
#include "mldb/arch/vm.h"

namespace MLDB {

// In suffix_array.h
std::string_view commonPrefix(const std::string_view & s1, const std::string_view & s2, uint32_t maxLen);

/* A trie is an associative array between strings and integers. So conceptually, it's a
   std::map<std::string, int>. However, where the keys have prefixes in common, it's much
   more efficient than a std::map, as the common prefixes are stored only once.

   It is stored as a tree structure. For efficiency over various kinds of data structures,
   we may store different parts in different ways. The core abstraction is a node, which:

   - Is reached by removing a prefix from the string to be matched (note that usually, the prefix is
     not stored in the node itself).
   - May have a value, which is the value associated with the prefix
   - May have sub-nodes, each of which is reached by a particular prefix

   This implementation creates a memory-mappable, static trie from a fixed list of key/value pairs.
   It attempts to be cache-friendly by densely packing data and minimizing linear pointer chasing.
 */
 
enum PrefixTrieNodeType: uint8_t {
    EMPTY,        ///< Empty node (needed to store empty tries)
    DENSE,        ///< Classic trie node; each of the 256 characters has a pointer to a sub-node
    TAIL,         ///< Tail node where there is one value associated with a (possibly empty) prefix
    MULTILEAF,    ///< Multiple values, each associated with a (possibly empty) prefix
    PREFIX,       ///< A single prefix leading to another node
    SPARSE,       ///< Sparse trie node; there are n child nodes, each associated with a different character
    SUCCINCT      ///< (under construction): a succinct data structure with multiple leaves
};

#define MLDB_FOR_EACH_PREFIX_TRIE_NODE_TYPE(op, ...) \
    op(EMPTY,     EmptyNode,      __VA_ARGS__) \
    op(DENSE,     DenseNode,      __VA_ARGS__) \
    op(TAIL,      TailNode,       __VA_ARGS__) \
    op(MULTILEAF, MultiLeafNode,  __VA_ARGS__) \
    op(PREFIX,    PrefixNode,     __VA_ARGS__) \
    op(SPARSE,    SparseNode,     __VA_ARGS__) \
    op(SUCCINCT,  SuccinctNode,   __VA_ARGS__) \

#define MLDB_DO_PREFIX_TRIE_TYPE_SWITCH(val, tp, node, typedNode, ...) \
case PrefixTrieNodeType::val: { const auto & typedNode = (node).template atOffset<tp>(0); __VA_ARGS__; break; } \


#define MLDB_PREFIX_TRIE_TYPE_SWITCH(node, typedNode, ...) \
switch ((PrefixTrieNodeType)(node).type_) { \
MLDB_FOR_EACH_PREFIX_TRIE_NODE_TYPE(MLDB_DO_PREFIX_TRIE_TYPE_SWITCH, node, typedNode, __VA_ARGS__) \
default: MLDB_THROW_LOGIC_ERROR(); \
} 

// Align a pointer to the alignment of a given type, possibly incrementing it to achieve the
// invariant.
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

template<typename Ch>
std::string toString(std::basic_string_view<Ch> s)
{
    std::string result;
    result.reserve(s.size());
    for (Ch c: s) {
        if (isascii(c))
            result += c;
        else
            result += "_";
    }
    return result;
}

inline std::string toString(std::string_view s)
{
    return std::string{s};
}

std::ostream & operator << (std::ostream & stream, const std::basic_string<char8_t> & s);
std::ostream & operator << (std::ostream & stream, const std::basic_string_view<char8_t> & s);

// Allocate a node plus following storage for its indirect data. The mapping context is
// informed as to where this memory came from.
template<typename Node, typename Ch>
Node & alloc_trie_node(MappingContext & context, std::basic_string_view<Ch> prefix, size_t extraBytes)
{
    // Allocate the given node, with extra space at the end, and 
    //static_assert(alignof(Node) == 1, "Only packed / byte aligned trie nodes can be allocated");
    Node & res = context.alloc_field<Node>(toString(prefix) + ".node");
    if (extraBytes)
        context.malloc(extraBytes);
    return res;
}

// Common header for all prefix trie nodes, so that they can be treated
// like polymorphic objects.
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


template<typename Ch, typename Base>
struct PrefixTrieImpl: Base {
private:
    using Base::data;
    using Base::dataLength;
    using NodeHeader = PrefixTrieNodeHeader;
    using StringViewType = std::basic_string_view<Ch>;
    using StringType = std::basic_string<Ch>;

    template<typename T> const T * atOffset(uint32_t offset) const
    {
        size_t endOffset;
        if (__builtin_add_overflow(offset, sizeof(T), &endOffset))
            MLDB_THROW_RANGE_ERROR("Overflow calculating end of range");
        ExcAssertLessEqual(endOffset, dataLength());
        return reinterpret_cast<const T *>(data() + offset);
    }

public:
    std::optional<uint32_t> get(const Ch * p) const
    {
        return this->getImpl(*atOffset<NodeHeader>(0), {p, std::char_traits<Ch>::length(p)});
    }

    std::optional<uint32_t> get(StringViewType s) const
    {
        return this->getImpl(*atOffset<NodeHeader>(0), s);
    }

    static constexpr std::pair<uint32_t, uint32_t> NO_LONGEST = { -1, 0 };

    // Find the longest entry in the trie that matches a prefix of the string s.
    // The first result is the value of the key; the second is the length of the
    // prefix that matches that key.
    std::pair<uint32_t, uint32_t> longest(StringViewType s) const
    {
        return this->longestImpl(*atOffset<NodeHeader>(0), s);
    }

    /// Lookup all prefix matches in the trie
    /// Parameters:
    ///   s (input): string to look up
    ///   res (output): array of nres pairs; first is trie node index, second is length matched
    ///   nres (input): length of res array
    ///
    /// Returns:
    ///   number of entries of res filled out (up to nres)
    ///
    /// If it returns nres, it's possible that the algorithm stopped due to lack of space for
    /// writing the output.

    size_t lookup(StringViewType s, std::pair<uint32_t, uint32_t> * res, size_t nres) const;

    size_t size() const
    {
        return this->size(*atOffset<NodeHeader>(0));
    }

    void dump(std::ostream & stream, size_t indent = 0) const
    {
        dumpImpl(*atOffset<NodeHeader>(0), stream, indent, 0 /* startAt */);
    }

    using VisitFn = std::function<void (StringViewType str, int)>;

    void visit(const VisitFn & fn) const
    {
        visitImpl(*atOffset<NodeHeader>(0), fn, {} /* prefix */, 0 /* startAt */);
    }

//private:
    template<typename Node>
    static std::optional<uint32_t>
    getImpl(const Node & node, StringViewType s)
    {
        if (s.empty()) {
            if (node.hasVal_)
                return 0;
            else
                return std::nullopt;
        }

        auto doNode = [&] (const auto & typedNode) { return typedNode.getChild(s); };
        MLDB_PREFIX_TRIE_TYPE_SWITCH(node, typedNode, return doNode(typedNode));
    }

    template<typename Node>
    static std::pair<uint32_t, uint32_t>
    longestImpl(const Node & node, StringViewType s)
    {
        if (s.empty()) {
            if (node.hasVal_)
                return { 0, 0 };
            else
                return NO_LONGEST;
        }

        uint32_t n, l;

        auto doNode = [&] (const auto & typedNode) { std::tie(n,l) = typedNode.longestImpl(s); };
        MLDB_PREFIX_TRIE_TYPE_SWITCH(node, typedNode, doNode(typedNode));

        if (n == (uint32_t)-1 && node.hasVal_) {
            return { 0, 0 };
        }

        return {n, l};
    }

    template<typename Node>
    static uint32_t size(const Node & node)
    {
        MLDB_PREFIX_TRIE_TYPE_SWITCH(node, typedNode, return typedNode.size(); );
    }

    static void dumpImpl(const NodeHeader & node, std::ostream & stream, size_t indent, size_t startAt)
    {
        MLDB_PREFIX_TRIE_TYPE_SWITCH(node, typedNode, typedNode.dumpImpl(stream, indent, startAt);  return);
    }

    static void visitImpl(const NodeHeader & node, const VisitFn & fn, StringViewType prefix, size_t startAt)
    {
        MLDB_PREFIX_TRIE_TYPE_SWITCH(node, typedNode, typedNode.visitImpl(fn, prefix, startAt);  return);
    }

    // Empty node, no sub-keys, may have a value
    struct EmptyNode: public NodeHeader {

        std::optional<uint32_t>
        getChild(StringViewType s) const
        {
            ExcAssert(!s.empty());
            return std::nullopt;
        }

        std::pair<uint32_t, uint32_t>
        longestImpl(StringViewType s) const
        {
            return NO_LONGEST;
        }

        size_t size() const { return hasVal_; }

        void dumpImpl(std::ostream & stream, size_t indent, size_t startAt) const
        {
            std::string i(indent, ' ');
            stream << i << "EMPTY";
            if (hasVal_)
                stream << " --> " << startAt;
            stream << std::endl;
        }

        void visitImpl(const VisitFn & fn, StringViewType prefix, size_t startAt) const
        {
            if (hasVal_)
                fn(prefix, startAt);
        }

        static auto & alloc(MappingContext & context, StringViewType prefix, bool hasValue)
        {
            auto & node = alloc_trie_node<EmptyNode>(context, prefix, 0 /* extra */);
            node.type_ = PrefixTrieNodeType::EMPTY;
            node.hasVal_ = hasValue;
            node.data_ = 0;
            return node;
        }
    };

    // Dense node, with (up to) 256 pointers, one per character
    struct DenseNode: public NodeHeader {
        using NodeHeader::atOffset;
        std::optional<uint32_t>
        getChild(StringViewType s) const
        {
            ExcAssert(!s.empty());
            uint32_t c = s[0];
            if (c >= offsets.size())
                return std::nullopt;
            auto ofs = offsets.at(c);
            if (ofs == 0)
                return std::nullopt;
            auto v = PrefixTrieImpl::getImpl(NodeHeader::template atOffset<NodeHeader>(ofs), s.substr(1));
            if (!v) return v;
            return v.value() + sizes.at(c) + hasVal_;
        }

        std::pair<uint32_t, uint32_t>
        longestImpl(StringViewType s) const
        {
            if (s.empty())
                return NO_LONGEST;
            uint32_t c = s[0];
            if (c >= offsets.size())
                return NO_LONGEST;
            auto ofs = offsets.at(c);
            if (ofs == 0)
                return NO_LONGEST;
            auto [n,l] = PrefixTrieImpl::longestImpl(NodeHeader::template atOffset<NodeHeader>(ofs), s.substr(1));
            if (n == -1)
                return NO_LONGEST;
            return { n + sizes.at(c) + hasVal_, l + 1 };
        }

        size_t size() const
        {
            return sizes.back();
#if 0
            size_t result = hasVal_;

            for (size_t i = 0;  i < offsets.size();  ++i) {
                auto ofs = offsets.at(i);
                if (ofs == 0)
                    continue;
                result += PrefixTrieImpl::size(NodeHeader::template atOffset<NodeHeader>(ofs));
            }
            return result;
#endif
        }

        void dumpImpl(std::ostream & stream, size_t indent, size_t startAt) const
        {
            using namespace std;
            std::string ind(indent, ' ');
            stream << ind << "DENSE size=" << offsets.size();
            if (hasVal_) {
                stream << " --> " << startAt;
                ++startAt;
            }
            stream << std::endl;

            //using namespace std;
            //cerr << "doing " << offsets.size() << endl;
            for (uint32_t i = 0;  i < offsets.size();  ++i) {
                auto ofs = offsets.at(i);
                if (ofs == 0)
                    continue;
                auto sz = sizes.at(i);
                stream << ind << "  '" << Ch(i) << "':"
                       << " ofs " << ofs
                       << std::endl;
                PrefixTrieImpl::dumpImpl(NodeHeader::template atOffset<NodeHeader>(ofs), stream, indent + 4, startAt + sz);
            }
        }

        void visitImpl(const VisitFn & fn, StringViewType prefix, size_t startAt) const
        {
            if (hasVal_)
                fn(prefix, startAt);

            for (uint32_t i = 0;  i < offsets.size();  ++i) {
                auto ofs = offsets.at(i);
                if (ofs == 0)
                    continue;
                auto sz = sizes.at(i);
                StringType newPrefix{prefix};
                newPrefix += Ch(i);
                PrefixTrieImpl::visitImpl(NodeHeader::template atOffset<NodeHeader>(ofs), fn, newPrefix, startAt + hasVal_ + sz);
            }
        }

        RawMappedIntTable offsets;
        RawMappedIntTable sizes;

        static auto & alloc(MappingContext & context, std::basic_string_view<Ch> prefix, bool hasValue)
        {
            auto & node = alloc_trie_node<DenseNode>(context, prefix, 0 /* extra */);
            node.type_ = PrefixTrieNodeType::DENSE;
            node.hasVal_ = hasValue;
            node.data_ = 0;
            return node;
        }
    };

    // Sparse node, with (up to) 32 pointers, one per character, slower lookup but more compact
    struct SparseNode: public NodeHeader {
        using NodeHeader::atOffset;

        std::optional<uint32_t>
        getChild(StringViewType s) const
        {
            ExcAssert(!s.empty());
            uint32_t c = s[0];

            auto beg = ch, end = ch + nch(), it = std::lower_bound(beg, end, c);
            if (it == end || *it != c)
                return std::nullopt;
            uint32_t i = it - beg;
            auto ofs = sizesAndOffsets.at(i * 2 + 1);

            auto v = PrefixTrieImpl::getImpl(NodeHeader::template atOffset<NodeHeader>(ofs), s.substr(1));
            if (!v) return v;
            auto sz  = sizesAndOffsets.at(i * 2);
            return hasVal_ + sz + v.value();
        }

        std::pair<uint32_t, uint32_t>
        longestImpl(StringViewType s) const
        {
            if (s.empty())
                return NO_LONGEST;

            uint32_t c = s[0];

            auto beg = ch, end = ch + nch(), it = std::lower_bound(beg, end, c);
            if (it == end || *it != c)
                return NO_LONGEST;
            uint32_t i = it - beg;
            auto ofs = sizesAndOffsets.at(i * 2 + 1);

            auto [n,l] = PrefixTrieImpl::longestImpl(NodeHeader::template atOffset<NodeHeader>(ofs), s.substr(1));
            if (n == -1) return NO_LONGEST;
            return { n + sizesAndOffsets.at(i * 2) + hasVal_, l + 1 };
        }

        size_t size() const
        {
            return sizesAndOffsets.back();
        }

        uint32_t nch() const
        {
            return sizesAndOffsets.size() / 2;
        }

        void dumpImpl(std::ostream & stream, size_t indent, size_t startAt) const
        {
            using namespace std;
            std::string ind(indent, ' ');
            stream << ind << "SPARSE size=" << size();
            if (hasVal_) {
                stream << " --> " << startAt;
                ++startAt;
            }
            stream << std::endl;
            for (size_t i = 0;  i < nch();  ++i) {
                auto sz = sizesAndOffsets.at(i * 2);
                auto ofs = sizesAndOffsets.at(i * 2 + 1);
                stream << ind << "  '" << ch[i] << "':"
                        << " ofs " << ofs << " size " << sz
                        << std::endl;
                PrefixTrieImpl::dumpImpl(NodeHeader::template atOffset<NodeHeader>(ofs), stream, indent + 4, startAt + sz);
            }
        }

        void visitImpl(const VisitFn & fn, StringViewType prefix, size_t startAt) const
        {
            if (hasVal_)
                fn(prefix, startAt);

            for (size_t i = 0;  i < nch();  ++i) {
                auto sz = sizesAndOffsets.at(i * 2);
                auto ofs = sizesAndOffsets.at(i * 2 + 1);
                StringType newPrefix{ prefix };
                newPrefix += ch[i];
                PrefixTrieImpl::visitImpl(NodeHeader::template atOffset<NodeHeader>(ofs), fn, newPrefix, startAt + hasVal_ + sz);
            }
        }

        MappedBitCompressedIntTable sizesAndOffsets;  ///< Data for table (2*n entries; even size, odd offset)
        Ch ch[0];        ///< Characters we're keyed off

        static auto & alloc(MappingContext & context,
                            std::basic_string_view<Ch> prefix,
                            bool hasValue,
                            uint32_t n)
        {
            auto & node = alloc_trie_node<SparseNode>(context, prefix, n * sizeof(Ch) /* extra */);
            node.type_ = PrefixTrieNodeType::SPARSE;
            node.hasVal_ = hasValue;
            node.data_ = 0;
            return node;
        }
    };

    // A chunk of prefix followed by a single value
    struct TailNode: public NodeHeader {
        using NodeHeader::atOffset;

        std::optional<uint32_t>
        getChild(StringViewType s) const
        {
            //using namespace std;
            //cerr << "tail getChild " << s << " " << getSuffix() << endl;
            if (hasVal_ && s.empty())
                return 0;
            if (s == getSuffix())
                return 0 + hasVal_;
            return std::nullopt;
        }

        std::pair<uint32_t, uint32_t>
        longestImpl(StringViewType s) const
        {
            if (s.starts_with(getSuffix())) {
                return { (uint32_t)hasVal_, len_ };
            }
            // Handled generically
            //if (hasVal_)
            //    return { 0, 0 };

            return NO_LONGEST;
        }

        size_t size() const
        {
            return 1 + hasVal_;
        }

        void dumpImpl(std::ostream & stream, size_t indent, size_t startAt) const
        {
            std::string i(indent, ' ');
            stream << i << "TAIL ";
            if (hasVal_)
                stream << "--> " << startAt << ", ";
            stream << getSuffix() << " --> ";
            stream << startAt + hasVal_ << std::endl;
        }

        void visitImpl(const VisitFn & fn, StringViewType prefix, size_t startAt) const
        {
            if (hasVal_)
                fn(prefix, startAt);
            StringType newPrefix{prefix};
            newPrefix += getSuffix();
            fn(newPrefix, startAt + hasVal_);
        }

        uint8_t len_;
        Ch tailChars_[0];

        std::basic_string_view<Ch> getSuffix() const
        {
            return { tailChars_, len_ };
        }

        static auto & alloc(MappingContext & context,
                            std::basic_string_view<Ch> prefix,
                            std::basic_string_view<Ch> key,
                            bool hasValue)
        {
            size_t n = key.size() - prefix.size();
            auto & node = alloc_trie_node<TailNode>(context, prefix, n /* extra */);
            node.type_ = PrefixTrieNodeType::TAIL;
            node.hasVal_ = hasValue;
            node.data_ = 0;
            ExcAssertLess(n, 256);
            node.len_ = n;
            std::copy(key.data() + prefix.size(), key.data() + key.size(), node.tailChars_);
            return node;
        }
    };

    // A chunk of prefix followed by another node
    struct PrefixNode: public NodeHeader {
        using NodeHeader::atOffset;

        std::optional<uint32_t>
        getChild(StringViewType s) const
        {
            if (hasVal_ && s.empty())
                return 0;
            if (!s.starts_with(getPrefix()))
                return std::nullopt;
            auto v = PrefixTrieImpl::getImpl(getNode(), s.substr(len_));
            if (!v)
                return std::nullopt;
            return v.value() + hasVal_;
        }

        std::pair<uint32_t, uint32_t>
        longestImpl(StringViewType s) const
        {
            if (!s.starts_with(getPrefix()))
                return NO_LONGEST;
            auto [n,l] = PrefixTrieImpl::longestImpl(getNode(), s.substr(len_));
            if (n == -1)
                return NO_LONGEST;
            return { n + hasVal_, l + len_ };
        }

        size_t size() const
        {
            return hasVal_ + PrefixTrieImpl::size(getNode());
        }

        void dumpImpl(std::ostream & stream, size_t indent, size_t startAt) const
        {
            std::string i(indent, ' ');
            stream << i << "PREFIX " << getPrefix();
            if (hasVal_)
                stream << "--> " << startAt;
            stream << std::endl;
            PrefixTrieImpl::dumpImpl(getNode(), stream, indent + 4, startAt + hasVal_);
        }

        void visitImpl(const VisitFn & fn, StringViewType prefix, size_t startAt) const
        {
            if (hasVal_)
                fn(prefix, startAt);
            
            StringType newPrefix{prefix};
            newPrefix += getPrefix();
            PrefixTrieImpl::visitImpl(getNode(), fn, newPrefix, startAt + hasVal_);
        }

        uint8_t len_;
        uint8_t nodeOfs_;
        Ch prefixChars_[0];

        std::basic_string_view<Ch> getPrefix() const
        {
            return { prefixChars_, len_ };
        }

        const NodeHeader & getNode() const
        {
            return *reinterpret_cast<const NodeHeader *>(prefixChars_ + len_ + nodeOfs_);
        }

        static auto & alloc(MappingContext & context,
                            std::basic_string_view<Ch> prefix,
                            std::basic_string_view<Ch> key,
                            bool hasValue)
        {
            size_t n = key.size() - prefix.size();
            auto & node = alloc_trie_node<PrefixNode>(context, prefix, n /* extra */);
            node.type_ = PrefixTrieNodeType::PREFIX;
            node.hasVal_ = hasValue;
            node.data_ = 0;
            ExcAssertLess(n, 256);
            node.len_ = n;
            std::copy(key.data() + prefix.size(), key.data() + key.size(), node.prefixChars_);
            return node;
        }
    };

    // Node with multiple leaves, with up to 255 characters in total length
    struct MultiLeafNode: public NodeHeader {

        uint8_t numLeaves_;
        uint8_t strs_[0];  // numLeaves offsets followed by compacted string data

        std::optional<uint32_t>
        getChild(StringViewType s) const
        {
            //using namespace std;
            //cerr << "multi getChild " << s << endl;

            ExcAssert(!s.empty());
            if (s.empty()) {
                if (hasVal_)
                    return 0;
                else return std::nullopt;
            }

            auto it = std::lower_bound(begin(), end(), s);
            if (it != end() && *it == s)
                return it.position() + hasVal_;
            return std::nullopt;
        }

        std::pair<uint32_t, uint32_t>
        longestImpl(StringViewType s) const
        {
            if (s.empty())
                return NO_LONGEST;

            // Find the lower bound
            auto it = std::lower_bound(begin(), end(), s);

            // Adjust as strings with a suffix will find a lower bound after their prefixes
            while (it != begin() && s.starts_with(*std::prev(it)))
                --it;

            uint32_t n = -1;
            uint32_t l = 0;
            while (it != end() && s.starts_with(*it)) {
                n = it.position() + hasVal_;
                l = (*it).length();
                ++it;
            }

            return { n, l };
        }

        size_t size() const { return hasVal_ + numLeaves_; }

        uint8_t getLeafOffset(uint32_t n) const
        {
            if (n == 0)
                return 0;
            ExcAssertLessEqual(n, (uint32_t)numLeaves_);
            return strs_[n - 1];
        }

        const Ch * getLeafData() const
        {
            return reinterpret_cast<const Ch *>(strs_ + numLeaves_);
        }

        Ch * getLeafData()
        {
            return reinterpret_cast<Ch *>(strs_ + numLeaves_);
        }

        std::basic_string_view<Ch> get(int n) const
        {
            auto ofs1 = getLeafOffset(n);
            auto ofs2 = getLeafOffset(n + 1);
            ExcAssertGreater(ofs2, ofs1);
            return { getLeafData() + ofs1, size_t(ofs2 - ofs1) };
        }

        using Iterator = StringTableIterator<MultiLeafNode, StringViewType>;
        Iterator begin() const { return { this, 0 }; }
        Iterator end() const { return { this, numLeaves_ }; }

        void dumpImpl(std::ostream & stream, size_t indent, size_t startAt) const
        {
            std::string idt(indent, ' ');
            stream << idt << "LEAVES size=" << (int)numLeaves_;
            if (hasVal_) {
                stream << " --> " << startAt;
                ++startAt;
            }
            stream << std::endl;
            for (size_t i = 0;  i < numLeaves_;  ++i) {
                stream << idt << "    " << get(i) << " --> " << startAt + i << std::endl;
            }
            stream << std::endl;
        }

        void visitImpl(const VisitFn & fn, StringViewType prefix, size_t startAt) const
        {
            if (hasVal_)
                fn(prefix, startAt);

            for (size_t i = 0;  i < numLeaves_;  ++i) {
                StringType newPrefix{prefix};
                newPrefix += get(i);
                fn(newPrefix, startAt + hasVal_ + i);
            }
        }

        static auto & alloc(MappingContext & context, std::basic_string_view<Ch> prefix,
                            size_t extraBytes, bool hasValue)
        {
            auto & node = alloc_trie_node<MultiLeafNode>(context, prefix, extraBytes);
            node.type_ = PrefixTrieNodeType::MULTILEAF;
            node.hasVal_ = hasValue;
            node.data_ = 0;
            return node;
        }
    };

    // Node with multiple leaves, succinct data structure
    struct SuccinctNode: public NodeHeader {

        uint16_t numLeaves_;
        uint16_t numWords_;
        union {
            uint64_t u64[0];
            Ch ch[0];
        };

        std::optional<uint32_t>
        getChild(StringViewType s) const
        {
            MLDB_THROW_UNIMPLEMENTED();
        }

        std::pair<uint32_t, uint32_t>
        longestImpl(StringViewType s) const
        {
            MLDB_THROW_UNIMPLEMENTED();
        }

        size_t size() const { return hasVal_ + numLeaves_; }

        const Ch * getLeafData() const
        {
            return reinterpret_cast<const Ch *>(u64 + numWords_);
        }

        Ch * getLeafData()
        {
            return reinterpret_cast<const Ch *>(u64 + numWords_);
        }

        void dumpImpl(std::ostream & stream, size_t indent, size_t startAt) const
        {
            MLDB_THROW_UNIMPLEMENTED();
        }

        void visitImpl(const VisitFn & fn, StringViewType prefix, size_t startAt) const
        {
            MLDB_THROW_UNIMPLEMENTED();
        }

        template<typename It>
        static bool doLevel(std::vector<std::byte> & mem, std::basic_string_view<Ch> prefix, int level, It begin, It end)
        {
            using namespace std;

            ExcAssert(begin != end);
            uint64_t prev = -1;

            // Skip the first empty prefix at the start
            It start = begin;
            if (start != end && start->first.size() == prefix.size() + level)
                ++start;

            // Find all distinct prefixes of length level in the range (begin, end]
            for (auto it = begin;  true;  /* no inc */) {
                ExcAssertGreater(it->first.size(), prefix.size() + level);
                Ch curr = it->first.at(prefix.size() + level);
                //cerr << "at " << it->first << " curr " << curr << endl;
                if (curr != prev || std::next(it) == end) {
                    // We got a new prefix character.  Add it to the trie
                    uint32_t numChildren = 0;

                    uint64_t prev2 = -1;

                    // Count the number of unique children at this level
                    It it2 = start;

                    while (it2 != it && it2->first.size() == prefix.size() + level)
                        ++it2;

                    //numChildren = std::distance(it2, it);

                    for (; it2 != it;  ++it2) {
                        Ch curr2 = it2->first.at(prefix.size() + level + 1);
                        if (curr2 != prev2) {
                            cerr << "  child " << curr2 << " " << it2->first << endl;
                            ++numChildren;
                            prev2 = curr2;
                        }
                    }

                    ExcAssert(it2 == it);

                    cerr << "got prefix " << Ch(prev) << " with " << numChildren << " children" << endl;

                    ++it;
                    if (it == end)
                        break;
                    start = it;
                }
                else {
                    ++it;
                }
                prev = curr;
            }

            return false;
        }

        template<typename It>
        static auto & alloc(MappingContext & context, std::basic_string_view<Ch> prefix,
                            bool hasValue, It first, It last)
        {
            std::vector<std::byte> mem;

            for (int level = 0;  doLevel(mem, prefix, level, first, last);  ++level) {}

            size_t extraBytes = 0;
            auto & node = alloc_trie_node<SuccinctNode>(context, prefix, extraBytes);
            node.type_ = PrefixTrieNodeType::SUCCINCT;
            node.hasVal_ = hasValue;
            node.data_ = 0;
            return node;
        }
    };
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

struct CharPrefixTrie: public PrefixTrieImpl<char, MemPrefixTrieBase> {};

// Insert the entries between first and last sorted (iterators over pair<stringlike, value>)
// into a newly constructed node on output, ignoring the prefixLen first characters in each
// string key.  Returns the offset of the node in the mapping context's arena.
template<typename It, typename Ch>
inline size_t
construct_trie_node(MappingContext & context, std::basic_string_view<Ch> prefix, It first, It last)
{
    using namespace std;

    size_t prefixLen = prefix.size();
    //cerr << "prefix.size() = " << prefix.size() << endl;

    size_t n = std::distance(first, last);

    using Val = std::decay_t<decltype(first->second)>;

    if (n == 0) {
        auto & node = CharPrefixTrie::EmptyNode::alloc(context, prefix, false /* hasValue */);
        return context.getOffset(&node);
    }
    else if (n == 1 && first->first.size() == prefix.size()) {
        // Just a value
        auto & node = CharPrefixTrie::EmptyNode::alloc(context, prefix, true /* hasValue */);
        return context.getOffset(&node);
    }
    else if (n == 1) {
        auto & node = CharPrefixTrie::TailNode::alloc(context, prefix, first->first, false /* hasValue */);
        return context.getOffset(&node);
    }
    else if (n == 2 && first->first.size() == prefix.size()) {
        auto & node = CharPrefixTrie::TailNode::alloc(context, prefix, (++first)->first, true /* hasValue */);
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

    size_t nch = 0;
    size_t nleft = 0;
    int64_t lastCh = -1;
    for (auto it = first;  it != last;  ++it, ++nleft) {
        Ch ch = it->first.at(prefixLen);
        if (ch == lastCh)
            continue;
        //cerr << "got char " << ch << endl;
        ++nch;
        lastCh = ch;
    }

    if (nch == 1) {
        // Common prefix
        size_t commonPrefixLength = first->first.size() - prefixLen;
        std::basic_string_view<Ch> sv1 = std::basic_string_view<Ch>(first->first).substr(prefixLen);
        for (auto it = first;  it != last && commonPrefixLength > 1;  ++it) {
            auto sv2 = std::basic_string_view<Ch>(it->first).substr(prefixLen);
            commonPrefixLength = std::min(commonPrefixLength, commonPrefix(sv1, sv2, commonPrefixLength).size());
        }
        //cerr << "opportunity for common prefix of length " << commonPrefixLength << " with " << nleft << " vals"
        //     << " saving " << commonPrefixLength * nleft * sizeof(Ch) << " chars" << endl;

        std::basic_string_view<Ch> nextPrefix = std::basic_string_view<Ch>(first->first).substr(0, prefixLen + commonPrefixLength);
        //cerr << "prefix = " << prefix << " nextPrefix = " << nextPrefix << endl;

        auto & node = CharPrefixTrie::PrefixNode::alloc(context, prefix, nextPrefix, val.has_value());
        size_t ofs = context.getOffset(&node);
        size_t nextOfs = construct_trie_node(context, nextPrefix, first, last);
        size_t startOfs = context.getOffset(node.prefixChars_ + node.len_);
        //cerr << "startOfs = " << startOfs << " nodeOfs = " << nextOfs - startOfs << endl;
        node.nodeOfs_ = nextOfs - startOfs;
        return ofs;
    }

    if (totalLen < 256 && nleft <= 16) {
        size_t extraBytes = totalLen + nleft;
        auto & node = CharPrefixTrie::MultiLeafNode::alloc(context, prefix, extraBytes, val.has_value());
        node.numLeaves_ = nleft;
        uint8_t ofs = 0;
        int i = 0;
        for (auto it = first;  it != last;  ++it, ++i) {
            size_t startofs = ofs;
            size_t len = it->first.size() - prefixLen;
            ofs += len;
            node.strs_[i] = ofs;
            std::memcpy(node.getLeafData() + startofs, it->first.data() + prefixLen, len);
        }
        return context.getOffset(&node);
    }

    auto getCh = [&] () -> uint32_t { return first->first.at(prefixLen); };

    if (false) {  // Succinct nodes not yet ready
        auto & node = CharPrefixTrie::SuccinctNode::alloc(context, prefix, val.has_value(), first, last);
        size_t startOffset = context.getOffset(&node);
        MLDB_THROW_UNIMPLEMENTED();
        return startOffset;
    }
    else if (nleft <= 32) {
        // Sparse node
        auto & node = CharPrefixTrie::SparseNode::alloc(context, prefix, val.has_value(), nleft);
        size_t startOffset = context.getOffset(&node);
        std::vector<uint32_t> sizesAndOffsets;
        sizesAndOffsets.reserve(2 * nleft + 1);

        auto orig = first;

        uint32_t i = 0;
        while (first != last) {
            size_t sz = std::distance(orig, first);
            //cerr << "doing " << first->first << " with prefix " << prefix << endl;
            ExcAssert(first->first.find(prefix) == 0);
            if (first->first.size() == prefix.size())
                continue;  // skip the value associated with the empty key
            auto it = first;
            auto c = getCh();
            std::basic_string_view<Ch> newPrefix(first->first.data(), prefix.size() + 1);
            //cerr << "str = " << first->first << " prefix " << prefix << " newPrefix = " << newPrefix << endl;

            ++first;
            while (first != last && getCh() == c)
                ++first;
            //cerr << "c = " << (int)c << endl;
            ssize_t ofs = construct_trie_node(context, newPrefix, it, first) - startOffset;
            ExcAssertGreater(ofs, 0);

            node.ch[i] = c;
            sizesAndOffsets.push_back(sz);
            sizesAndOffsets.push_back(ofs);
            ++i;
        }

        sizesAndOffsets.emplace_back(nleft);
        freeze(context, node.sizesAndOffsets, sizesAndOffsets);

        return startOffset;
    }
    else {
        // Dense node
        auto & node = CharPrefixTrie::DenseNode::alloc(context, prefix, val.has_value());
        size_t startOffset = context.getOffset(&node);

        std::vector<uint32_t> offsets;
        offsets.reserve(256);
        std::vector<uint32_t> sizes;
        sizes.reserve(256);

        auto orig = first;

        while (first != last) {
            size_t sz = std::distance(orig, first);
            //cerr << "doing " << first->first << " with prefix " << prefix << endl;
            ExcAssert(first->first.find(prefix) == 0);
            if (first->first.size() == prefix.size())
                continue;  // skip the value associated with the empty key
            auto it = first;
            auto c = getCh();
            std::basic_string_view<Ch> newPrefix(first->first.data(), prefix.size() + 1);
            //cerr << "str = " << first->first << " prefix " << prefix << " newPrefix = " << newPrefix << endl;

            ++first;
            while (first != last && getCh() == c)
                ++first;
            //cerr << "c = " << (int)c << endl;
            size_t ofs = construct_trie_node(context, newPrefix, it, first) - startOffset;
            ExcAssertGreater(ofs, 0);
            //cerr << "done constructing " << std::distance(it, first) << " with prefix " << newPrefix << " at offset " << ofs << endl;
            if (c >= offsets.size()) {
                offsets.resize(c + 1);
                sizes.resize(c + 1);
            }
            offsets[c] = ofs;
            sizes[c] = sz;
        }

        sizes.emplace_back(std::distance(orig, first));

        //cerr << "freezing " << offsets.size() << " offsets for " << prefix << endl;

        freeze(context, node.offsets, offsets);
        freeze(context, node.sizes, sizes);

        return startOffset;
    }
    //MLDB_THROW_UNIMPLEMENTED();
}

inline CharPrefixTrie construct_trie(const std::map<std::string, uint16_t> & m)
{
    std::vector<uint32_t> commonPrefixLengths;
    if (!m.empty()) {
        commonPrefixLengths.reserve(m.size() - 1);
        for (auto it = m.begin(); it != m.end();  /* no inc */) {
            std::string_view last = (it++)->first;
            if (it == m.end())
                break;
            std::string_view next = it->first;
            commonPrefixLengths.emplace_back(commonPrefix(last, next, MAX_LIMIT).length());
        }
    }

    CharPrefixTrie result;
    size_t totalLength = 0;
    for (auto & [key, val]: m) {
        totalLength += key.size();
    }

    size_t toAllocate = 256 + 24 * m.size() + totalLength;

    MappingContext context(roundUpToPageSize(toAllocate)
    //                       , DUMP_TYPE_STATS=true
    //                       , DUMP_MEMORY_MAP=true
                           );
    uint32_t ofs = construct_trie_node(context, std::string_view{}, m.begin(), m.end());
    ExcAssertEqual(ofs, 0);

#if 0
    using namespace std;
    cerr << "allocated " << context.getOffset() << " bytes of " << toAllocate
         << " for " << m.size() << " entries with " << totalLength << " key bytes" << endl;
#endif
    result.mem = { context.getMemory(), context.getMemory() + context.getOffset() };
    return result;
}

} // namespace MLDB
