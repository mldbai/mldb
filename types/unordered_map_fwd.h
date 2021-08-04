#pragma once

#include "mldb/compiler/stdlib.h"

namespace std {

#ifdef MLDB_STDLIB_GCC
template<class K, class V, class H, class P, class A>
class unordered_map;
#elif MLDB_STDLIB_LLVM
inline namespace __1 {
template<class K, class V, class H, class P, class A>
class unordered_map;
}
#else
#  error "Tell us how to forward declare set for your standard library"
#endif

}
