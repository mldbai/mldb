#pragma once

#include <ciso646>

namespace std {

#ifdef __GLIBCXX__
template<class T, class A>
class vector;
#elif defined(_LIBCPP_VERSION)
inline namespace __1 {
template<class T, class A>
class vector;
}
#endif

}
