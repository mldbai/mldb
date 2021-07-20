#pragma once

#include <ciso646>

namespace std {

#ifdef __GLIBCXX__
template<class V, class L, class A>
class set;
#elif defined(_LIBCPP_VERSION)
inline namespace __1 {
template<class V, class L, class A>
class set;
}
#endif

}