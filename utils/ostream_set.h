#pragma once

#include <set>
#include <iostream>

template<typename T, typename Cmp, typename Alloc>
std::ostream &
operator << (std::ostream & stream, const std::set<T, Cmp, Alloc> & s)
{
    stream << "{";
    for (const auto & e: s)
        stream << " " << e;
    return stream << " }";
}