#pragma once

#include <span>
#include <iostream>

template<typename T, size_t Size>
std::ostream &
operator << (std::ostream & stream, const std::span<T, Size> & s)
{
    stream << "[";
    for (const auto & e: s)
        stream << " " << e;
    return stream << " ]";
}

