#pragma once

#include <ciso646>

namespace std {

#ifdef __GLIBCXX__
template<class T1, class T2>
class pair;
#elif defined(_LIBCPP_VERSION)
inline namespace __1 {
template<class T1, class T2>
class pair;
}
#endif

} // namespace std
