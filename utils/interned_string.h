/** interned_string.h                                              -*- C++ -*-
    Jeremy Barnes, 29 January 2016
    Copyright (c) 2016 mldb.ai inc.  All rights reserved.

    This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.
*/

#include <string>
#include <cstring>
#include "mldb/base/exc_assert.h"
#include "mldb/compiler/compiler.h"

// NOTE TO MLDB DEVELOPERS: This is an API header file.  No includes
// should be added, especially value_description.h.

#pragma once


namespace MLDB {

/*****************************************************************************/
/* INTERNED STRING                                                           */
/*****************************************************************************/

template<size_t Bytes, typename Char = char>
struct InternedString {

    // Max Bytes is 255, as otherwise we can't determine the length
    static_assert(Bytes < 256, "First template parameter for InternedString "
                  "must be 255 or less");

    // Only POD types can be used so that Reserve can use memcpy and
    // not need to handle exceptions
    static_assert(std::is_pod<Char>::value, "InternedString can only hold POD "
                  "types");
    
    InternedString()
        : intLength_(0)
    {
    }

    InternedString(const InternedString & other)
        : InternedString()
    {
        append(other.data(), other.length());
    }

    template<size_t OtherBytes>
    InternedString(const InternedString<OtherBytes, Char> & other)
        : InternedString()
    {
        append(other.data(), other.length());
    }

    template<size_t OtherBytes>
    InternedString(InternedString<OtherBytes, Char> && other) noexcept
        : InternedString()
    {
        if (other.length() > Bytes) {
            // Can't fit internally.  If the other is external, steal it
            if (other.isExt()) {
                intLength_ = IS_EXT;
                extLength_ = other.extLength_;
                extCapacity_ = other.extCapacity_;
                extBytes_ = other.extBytes_;
                other.intLength_ = 0;
                return;
            }
        }

        // Otherwise, simply append it
        append(other.data(), other.size());
    }

    InternedString(const std::basic_string<Char> & other)
        : InternedString()
    {
        append(other.data(), other.length());
    }

    InternedString & operator = (const InternedString & other)
    {
        if (MLDB_UNLIKELY(&other == this))
            return *this;
        clear();
        append(other.data(), other.length());
        return *this;
    }

    InternedString & operator = (InternedString && other) noexcept
    {
        // NOTE: it's valid to assume that self move assignment will
        // never happen.

        // Kill earlier contents
        clear();
        
        if (MLDB_UNLIKELY(other.isExt())) {
            // Optimize to move pointer to data
            intLength_ = other.intLength_;
            extLength_ = other.extLength_;
            extCapacity_ = other.extCapacity_;
            extBytes_ = other.extBytes_;
            other.intLength_ = 0;
        }
        else {
            intLength_ = other.intLength_;
            std::memmove(intBytes_, other.intBytes_, intLength_);
        }
        return *this;
    }

    ~InternedString() noexcept
    {
        clear();
    }

    const Char * data() const noexcept
    {
        return isExt() ? extBytes_ : intBytes_;
    }

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wmaybe-uninitialized"
#pragma GCC diagnostic ignored "-Wuninitialized"
    void swap(InternedString & other) noexcept
    {
	// NOTE: this swap operator is complicated to avoid violating
        // strict aliasing rules.  For that reason, it's not used in
        // the implementation of any of the underlying functionality.

        if (MLDB_LIKELY(!this->isExt())) {
            if (MLDB_LIKELY(!other.isExt())) {
                // Both internal
                std::swap(intLength_, other.intLength_);
                std::swap(intBytes_[0], other.intBytes_[0]);
                std::swap(intBytes_[1], other.intBytes_[1]);
                std::swap(intBytes_[2], other.intBytes_[2]);
                // TODO: maybe could swap less words if the string is
                // short to improve performance
                for (unsigned i = 0;  i < NUM_WORDS * 2 - 1;  ++i) {
		    auto tmp = internalWords[i];
		    internalWords[i] = other.internalWords[i];
		    other.internalWords[i] = tmp;
                }
            }
            else {
                // Swapping an internal (this) and an external (other)
                // We copy the external, and use the move constructor
                InternedString tmp(std::move(other));
                other = std::move(*this);
                *this = std::move(tmp);
            }
        }
        else {
            if (MLDB_LIKELY(other.isExt())) {
                // std:swap needs addresses and gcc doesn't want to give
                // addresses of packed fields, so do it the long way here
#define MLDB_INTERNED_STRING_SWAP(x, y) do { auto t = x;  x = y;  y = t; } while (0)
                // Both external
                MLDB_INTERNED_STRING_SWAP(intLength_, other.intLength_);
                MLDB_INTERNED_STRING_SWAP(extLength_, other.extLength_);
                MLDB_INTERNED_STRING_SWAP(extCapacity_, other.extCapacity_);
                MLDB_INTERNED_STRING_SWAP(extBytes_, other.extBytes_);
#undef MLDB_INTERNED_STRING_SWAP
            }
            else {
                // Swapping an internal (other) and an external (this)
                // We copy the external, and use the move constructor
                InternedString tmp(std::move(*this));
                *this = std::move(other);
                other = std::move(tmp);
            }
        }
    }
#pragma GCC diagnostic pop

    constexpr size_t size() const noexcept
    {
        return isExt() ? extLength_ : intLength_;
    }

    constexpr size_t length() const noexcept
    {
        return size();
    }

    constexpr bool empty() const noexcept
    {
        return size() == 0;
    }

    void reserve(size_t newCapacity)
    {
        if (newCapacity < capacity())
            return;

        bool wasExt = isExt();

        char * newBytes = new Char[newCapacity];

        // No possibility of exception from here on because Char is a
        // POD type.  So we don't need to use a smart pointer to guarantee
        // that newBytes is destroyed.

        size_t l = size();
        std::memcpy(newBytes, data(), l);
        intLength_ = IS_EXT;
        extLength_ = l;
        extCapacity_ = newCapacity;

        if (wasExt)
            delete[] extBytes_;

        extBytes_ = newBytes;
    }

    size_t capacity() const noexcept
    {
        return isExt() ? extCapacity_ : Bytes;
    }

    void clear() noexcept
    {
        if (isExt())
            deleteExt();
        intLength_ = 0;
    }
    
    void append(const Char * start, const Char * end)
    {
        append(start, end - start);
    }

    void append(const Char * bytes, size_t n)
    {
        if (n + size() > capacity()) {
            reserve(std::max(capacity() * 2, capacity() + n));
        }
        ExcAssertGreaterEqual(capacity(), size() + n);
        std::memcpy((Char *)(data() + size()), bytes, n);
        if (isExt()) {
            extLength_ += n;
        }
        else intLength_ += n;
    }

    size_t externalMemusage() const noexcept
    {
        if (isExt())
            return length();
        return 0;
    }

    size_t memusage() const noexcept
    {
        return sizeof(InternedString) + externalMemusage();
    }
    
private:
    template<size_t OtherBytes, typename OtherChar>
    friend class InternedString;

public:
    bool isExt() const noexcept { return intLength_ == IS_EXT; }

private:
    void deleteExt()
    {
        delete[] extBytes_;
    }

    static constexpr uint8_t IS_EXT = 255;
    static constexpr size_t INTERNAL_BYTES = Bytes;
    static constexpr size_t NUM_WORDS = (Bytes + 9) / 8;

public:
    union {
        struct {
            // NOTE: these need to be OUTSIDE of the internal/external union
            // as otherwise clang gets undefined behavior 
            uint8_t intLength_;  // if -1, it's external.
            char intBytes_[3];
            union {
                struct {
                    Char restOfIntBytes_[Bytes - 3];
                } MLDB_PACKED;
                struct {
                    uint32_t extLength_;
                    uint32_t extCapacity_;
                    Char * extBytes_;
                } MLDB_PACKED;
                uint32_t internalWords[NUM_WORDS * 2 - 1];
            } MLDB_PACKED;
        } MLDB_PACKED;
    };
} MLDB_PACKED;

} // namespace MLDB

