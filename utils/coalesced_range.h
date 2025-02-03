/* coalesced_range.h"

    Jeremy Barnes, 29 November 2013
    Copyright (c) 2013 mldb.ai inc.  All rights reserved.

    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

    Class to process each line in a file in parallel.
*/

#pragma once

#include "coalesced_range_fwd.h"
#include <vector>
#include <string>
#include <memory>
#include <span>
#include <algorithm>
#include <optional>
#include "mldb/base/exc_assert.h"
#include "mldb/base/exc_check.h"
#include "mldb/compiler/compiler.h"

namespace MLDB {

template<typename Value, typename Ranges>
struct CoalescedRangeIterator {
    using value_type = Value;
    using difference_type = ssize_t;
    using pointer = Value *;
    using reference = Value &;
    using iterator_category = std::random_access_iterator_tag;
    using ranges_type = Ranges;

    auto operator <=> (const CoalescedRangeIterator & other) const = default;

    size_t range_size() const
    {
        ExcCheck(ranges_, "Coalesced range iterator not initialized");
        ExcCheckLess(range_, ranges_->size(), "Coalesced range iterator past end");
        return (*ranges_)[range_].size();
    }

    CoalescedRangeIterator & operator ++ ()
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

    CoalescedRangeIterator operator ++ (int)
    {
        auto result = *this;
        operator ++ ();
        return result;
    }

    CoalescedRangeIterator & operator -- ()
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

    CoalescedRangeIterator operator -- (int)
    {
        auto result = *this;
        operator -- ();
        return result;
    }

    CoalescedRangeIterator & operator += (ssize_t ofs)
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

    CoalescedRangeIterator & operator -= (ssize_t ofs)
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

    CoalescedRangeIterator operator + (ssize_t ofs) const { CoalescedRangeIterator result = *this; result += ofs; return result; }
    CoalescedRangeIterator operator - (ssize_t ofs) const { CoalescedRangeIterator result = *this; result -= ofs; return result; }

    ssize_t operator - (const CoalescedRangeIterator & other) const
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

    typename ranges_type::iterator ranges_begin() const
    {
        ExcCheck(ranges_, "Attempt to dereference null coalesced range iterator");
        return ranges_->begin();
    }

    typename ranges_type::iterator ranges_end() const
    {
        ExcCheck(ranges_, "Attempt to dereference null coalesced range iterator");
        return ranges_->end();
    }

    size_t block_number() const { return range_; }
    size_t block_offset() const { return offset_; }
    bool at_the_end() const { return range_ == ranges_->size(); }

    // Used in the implementation of some algorithms which work better if the off_the_end is represented
    // by a range_ pointing to the last range and offset_ pointing to the end of that range. Note that
    // the iterator to which this is done is no longer usable as an iterator.

    void flip_ate() { if (at_the_end() && range_ > 0) { --range_; offset_ = range_size(); } }

    template<typename T, typename R> friend struct CoalescedRange;
};

template<typename T, typename Ranges>
struct CoalescedRange {
    using ranges_type = Ranges;
    using range_type = typename ranges_type::value_type;
    using value_type = T;

    using iterator = CoalescedRangeIterator<value_type, ranges_type>;
    using const_iterator = CoalescedRangeIterator<const value_type, ranges_type>;

    iterator begin() { return {&ranges_, 0, 0}; }
    iterator end()   { return {&ranges_, ranges_.size(), 0}; }
    const_iterator begin() const { return {&ranges_, 0, 0}; }
    const_iterator end()   const { return {&ranges_, ranges_.size(), 0}; }

    // Add something to the ranges. Invalidates iterators.
    // If it is empty, it will not be added.
    // If it is an extension of the previous range, then it will be extended.
    // Returns true if and only if the length of ranges_ increased
    template<typename... Args> bool add(Args&&... args) { return add(range_type(std::forward<Args>(args)...)); }

    bool add(range_type range)
    {
        if (range.empty())
            return false;
        else if (!ranges_.empty() && ranges_.back().data() + ranges_.back().size() == range.data()) {
            // Contiguous, combine them
            ranges_.back() = {ranges_.back().data(), ranges_.back().size() + range.size()};
            return false;
        }
        else {
            ranges_.push_back(range);
            return true;
        }
    }

    size_t size() const { size_t result = 0; for (auto r: ranges_) result += r.size(); return result; }
    size_t range_count() const { return ranges_.size(); }

    // Convert to a string, rolling up all of the ranges
    std::basic_string<std::remove_cv_t<value_type>> to_string() const
    {
        std::basic_string<std::remove_cv_t<value_type>> result;
        for (auto r: ranges_)
            result.append(r.data(), r.size());
        return result;
    }

    std::basic_string<std::remove_cv_t<value_type>> to_string(const_iterator first, const_iterator last) const
    {
        ExcCheck(first.ranges_ == &ranges_ && last.ranges_ == &ranges_, "Attempt to convert span from wrong ranges");
        ExcCheck(first <= last, "Attempt to convert backwards span");

        size_t n = std::distance(first, last);
        std::basic_string<std::remove_cv_t<value_type>> result;
        result.reserve(n);
        for (size_t i = first.range_; i <= last.range_ && i < ranges_.size(); ++i) {
            auto start_ofs = i == first.range_ ? first.offset_ : 0;
            auto end_ofs = i == last.range_ ? last.offset_ : ranges_[i].size();
            result.append(ranges_[i].data() + start_ofs, ranges_[i].data() + end_ofs);
        }
        return result;
    }


    // Reduce it to contain just the mentioned range
    // It may lead to the list of ranges shrinking
    // The result gives the indexes (first and past-the-end) of the new range in the
    // old ranges.
    std::tuple<size_t, size_t> reduce(const_iterator begin, const_iterator end)
    {
        ExcCheck(begin <= end, "Attempt to reduce with backwards span");
        ExcCheck(begin.ranges_ == &ranges_ && end.ranges_ == &ranges_, "Attempt to reduce span from wrong ranges");

        if (begin == end) {
            ranges_.clear();
            return { begin.range_, begin.range_ };
        }

        size_t first = begin.block_number();
        size_t first_ofs = begin.block_offset();
        size_t last = end.block_number();
        size_t last_ofs = end.block_offset();
        if (last_ofs == 0) {
            ExcAssert(last > 0);
            last -= 1;
        }

        ranges_type new_ranges(ranges_.begin() + first, ranges_.begin() + last + 1);

        if (!new_ranges.empty()) {
            auto p1 = new_ranges.front().data();
            auto sz1 = new_ranges.front().size();
            p1 += first_ofs;
            sz1 -= first_ofs;
            new_ranges.front() = { p1, sz1 };

            auto p2 = new_ranges.back().data();
            auto sz2 = new_ranges.back().size();
            sz2 -= last_ofs;
            new_ranges.back() = { p2, sz2 };
        }

        ranges_.swap(new_ranges);

        return { first, last + 1 };
    }

    // Try to extract a single contiguous span of memory from two iterators.
    // If both iterators are contained within a contiguous span of memory, then return the
    // span of memory between them. Otherwise, a null option is returned.
    std::optional<range_type> get_span(const_iterator begin, const_iterator end) const
    {
        ExcCheck(begin <= end, "Attempt to extract backwards span");
        ExcCheck(begin.ranges_ == &ranges_ && end.ranges_ == &ranges_, "Attempt to extract span from wrong ranges");

        if (begin == end)
            return range_type();

        end.flip_ate();

        if (begin.range_ != end.range_)
            return std::nullopt;

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
        ExcAssert(first <= last);
        ExcAssert(first.ranges_);
        ExcAssert(first.ranges_ == last.ranges_);

        while (first < last) {
            auto rit = first.range_it();
            auto rend = first.range_ == last.range_ ? last.range_it() : first.range_end();

            // For chars, this should be optimized to memchr or similar
            auto rfound = std::find(rit, rend, what);
            if (rfound != rend) {
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
