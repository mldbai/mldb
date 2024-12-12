#pragma once

#include <utility>
#include <iostream>

template<typename X, typename Y>
std::ostream &
operator << (std::ostream & stream, const std::pair<X, Y> & a)
{
    return stream << "( " << a.first << ", " << a.second << ")";
}

