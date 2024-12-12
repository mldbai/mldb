#pragma once

#include <vector>
#include <iostream>

template<typename T, typename Alloc>
std::ostream &
operator << (std::ostream & stream, const std::vector<T, Alloc> & v)
{
    stream << "[";
    for (const auto & e: v)
        stream << " " << e;
    return stream << " ]";
}

