#pragma once

#include <ciso646>

namespace std {

#ifdef __GLIBCXX__
template<class K, class V, class L, class A>
class map;
#elif defined(_LIBCPP_VERSION)
inline namespace __1 {
template<class K, class V, class L, class A>
class map;    
}
#endif

} // namespace std
