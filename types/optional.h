// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/** optional.h                                                     -*- C++ -*-
    Jeremy Barnes, 19 August 2015
    Optional type.
*/

#pragma once

#include <memory>

namespace MLDB {

template<typename T>
struct Optional: public std::shared_ptr<T> {
    Optional()
    {
    }
 
    Optional(Optional && other)
        : std::shared_ptr<T>(std::move(other))
    {
    }

    Optional(const Optional & other)
    {
        if (other)
            this->reset(new T(*other));
    }

    Optional & operator = (const Optional & other)
    {
        Optional newMe(other);
        swap(newMe);
        return *this;
    }

    Optional & operator = (Optional && other)
    {
        Optional newMe(other);
        swap(newMe);
        return *this;
    }

    bool operator == (Optional && other) const
    {
        return (this->get() == other.get()
                || (this->get() != nullptr
                    && other.get != nullptr
                    && *this == *other));
    }

    void swap(Optional & other)
    {
        std::shared_ptr<T>::swap(other);
    }

    template<typename... Args>
    void emplace(Args&&... args)
    {
        this->reset(new T(std::forward<Args>(args)...));
    }
};


} // namespace MLDB
