/* coalesced_range.h"

    Jeremy Barnes, 29 November 2013
    Copyright (c) 2013 mldb.ai inc.  All rights reserved.

    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

    Class to process each line in a file in parallel.
*/

#pragma once

#include <vector>
#include <memory>
#include <span>
#include <algorithm>
#include <optional>
#include "mldb/base/exc_assert.h"
#include "mldb/base/exc_check.h"
#include "mldb/compiler/compiler.h"
#include <iostream>

namespace MLDB {

template<typename T, typename Ranges = std::vector<std::span<T>>>
struct CoalescedRange {
    using ranges_type = Ranges;
    using range_type = typename ranges_type::value_type;
    using value_type = T;

    template<typename Value> struct Iterator {
        using value_type = Value;
        using difference_type = ssize_t;
        using pointer = Value *;
        using reference = Value &;
        using iterator_category = std::random_access_iterator_tag;

        auto operator <=> (const Iterator & other) const = default;

        size_t range_size() const
        {
            ExcCheck(ranges_, "Coalesced range iterator not initialized");
            ExcCheckLess(range_, ranges_->size(), "Coalesced range iterator past end");
            return (*ranges_)[range_].size();
        }

        Iterator & operator ++ ()
        {
            ExcAssert(ranges_);
            ExcAssert(offset_ < range_size());
            ++offset_;
            if (MLDB_UNLIKELY(offset_ == range_size())) {
                ExcAssert(range_ < ranges_->size());
                ++range_;
                offset_ = 0;
            }
            return *this;
        }

        Iterator operator ++ (int)
        {
            auto result = *this;
            operator ++ ();
            return result;
        }

        Iterator & operator -- ()
        {
            ExcAssert(ranges_);
            if (MLDB_UNLIKELY(offset_ == 0)) {
                ExcAssert(range_ > 0);
                --range_;
                offset_ = range_size();
            }
            ExcAssert(offset_ > 0);
            --offset_;
            return *this;
        }

        Iterator operator -- (int)
        {
            auto result = *this;
            operator -- ();
            return result;
        }

        Iterator & operator += (ssize_t ofs)
        {
            if (ofs >= 0) {
                while (ofs) {
                    auto cs = range_size();
                    if (ofs >= cs - offset_) {
                        ofs -= cs - offset_;
                        offset_ = 0;
                        ++range_;
                    }
                    else {
                        offset_ += ofs;
                        ofs = 0;
                    }
                }
                return *this;
            }
            else return operator -= (ofs);
        }

        Iterator & operator -= (ssize_t ofs)
        {
            if (ofs >= 0) {
                while (ofs) {
                    if (ofs >= offset_) {
                        ofs -= offset_;
                        ExcCheckGreater(range_, 0, "Coalesced range iterator before beginning");
                        --range_;
                        offset_ = range_size();
                    }
                    else {
                        offset_ -= ofs;
                        ofs = 0;
                    }
                }
                return *this;
            }
            else return operator += (-ofs);
        }

        Iterator operator + (ssize_t ofs) const { Iterator result = *this; result += ofs; return result; }
        Iterator operator - (ssize_t ofs) const { Iterator result = *this; result -= ofs; return result; }

        ssize_t operator - (const Iterator & other) const
        {
            ExcCheck(ranges_ == other.ranges_, "Attempt to subtract non-compatible range iterators");

            if (range_ < other.range_)
                return - other.operator-(*this);

            ssize_t result = -other.offset_;
            for (auto r = other.range_; r < range_; ++r) {
                result += (*ranges_)[r].size();
            }
            result += offset_;
            return result;
        }

        value_type & operator * () const
        {
            ExcCheck(ranges_, "Attempt to dereference null coalesced range iterator");
            ExcCheckLess(range_, ranges_->size(), "Attempt to dereference off-the-end coalesced range iterator");
            return (*ranges_)[range_][offset_];
        }

        value_type & operator [] (ssize_t ofs) const
        {
            return *(*this + ofs);
        }

        const ranges_type * ranges_ = nullptr;
        size_t range_ = 0;
        size_t offset_ = 0; // invariant: offset_ will never point past the end of a valid range_ value

        const value_type * range_begin() const
        {
            ExcCheck(ranges_, "Attempt to dereference null coalesced range iterator");
            return ranges_->at(range_).data();
        }

        const value_type * range_it() const { return range_begin() + offset_; }
        const value_type * range_end() const { return range_begin() + range_size(); }

        friend struct CoalescedRange;
    };

    using iterator = Iterator<value_type>;
    using const_iterator = Iterator<const value_type>;

    iterator begin() { return {&ranges_, 0, 0}; }
    iterator end()   { return {&ranges_, ranges_.size(), 0}; }
    const_iterator begin() const { return {&ranges_, 0, 0}; }
    const_iterator end()   const { return {&ranges_, ranges_.size(), 0}; }

    // Add something to the ranges. Invalidates iterators.
    // If it is empty, it will not be added.
    // If it is an extension of the previous range, then it will be extended.
    template<typename... Args> void add(Args&&... args) { return add(range_type(std::forward<Args>(args)...)); }

    void add(range_type range)
    {
        if (range.empty())
            return;
        else if (!ranges_.empty() && ranges_.back().data() + ranges_.back().size() == range.data()) {
            // Contiguous, combine them
            ranges_.back() = {ranges_.back().data(), ranges_.back().size() + range.size()};
        }
        else {
            ranges_.push_back(range);
        }
    }

    size_t size() const { size_t result = 0; for (auto r: ranges_) result += r.size(); return result; }

    void reduce(const_iterator begin, const_iterator end);

    // Try to extract a single contiguous span of memory from two iterators.
    // If both iterators are contained within a contiguous span of memory, then return the
    // span of memory between them. Otherwise, a null option is returned.
    std::optional<range_type> get_span(const_iterator begin, const_iterator end) const
    {
        ExcCheck(begin < end, "Attempt to extract backwards span");
        ExcCheck(begin.ranges_ == &ranges_ && end.ranges_ == &ranges_, "Attempt to extract span from wrong ranges");
        if (begin.range_ != end.range_)
            return std::nullopt;
        if (begin.range_ == ranges_.size())
            return range_type();
        auto first = begin.range_it();
        auto last = end.range_it();
        return range_type(first, last - first);    
    }

    iterator find(const value_type & what) { return find(begin(), end(), what); }
    const_iterator find(const value_type & what) const { return find(begin(), end(), what); }

    iterator find(iterator first, iterator last, const value_type & what) { return find_impl(first, last, what); }
    const_iterator find(iterator first, iterator last, const value_type & what) const { return find_impl(first, last, what); }

private:
    template<typename Iterator>
    static Iterator find_impl(Iterator first, const Iterator & last, const value_type & what)
    {
        using namespace std;

        ExcAssert(first <= last);
        ExcAssert(first.ranges_);
        ExcAssert(first.ranges_ == last.ranges_);

        cerr << endl;
        cerr << "find: looking between " << first.range_ << "," << first.offset_ << " and " << last.range_ << "," << last.offset_ << endl;

        while (first < last) {
            cerr << "looking within range " << first.range_ << ": " << std::string(first.range_begin(), first.range_end()) << endl;
            auto rit = first.range_it();
            auto rend = first.range_ == last.range_ ? last.range_it() : first.range_end();

            cerr << "  chars in play: " << std::string(rit, rend) << endl;

            // For chars, this should be optimized to memchr or similar
            auto rfound = std::find(rit, rend, what);
            if (rfound != rend) {
                cerr << "found at pos " << rfound - first.range_begin() << endl;
                ExcAssert(*rfound == what);
                // Found here
                return Iterator{first.ranges_, first.range_, first.offset_ + std::distance(rit, rfound)};
            }

            // Skip to the next one
            ++first.range_;
            first.offset_ = 0;
        }

        return last;
    }

    ranges_type ranges_;
};

} // namespace MLDB
