#pragma once

#include <ciso646>

namespace std {

#ifdef __GLIBCXX__
template<class K, class V, class H, class P, class A>
class unordered_map;
#elif defined(_LIBCPP_VERSION)
inline namespace __1 {
template<class K, class V, class H, class P, class A>
class unordered_map;
}
#endif

}
