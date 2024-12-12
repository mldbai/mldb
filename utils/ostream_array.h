#pragma once

#include <array>
#include <iostream>

namespace std {

template<typename T, size_t N>
std::ostream &
operator << (std::ostream & stream, const std::array<T, N> & a)
{
    stream << "[";
    for (unsigned i = 0;  i < a.size();  ++i) {
        stream << " " << a[i];
    }
    return stream << " ]";
}

} // namespace std