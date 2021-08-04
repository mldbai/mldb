#pragma once

#include "mldb/compiler/stdlib.h"

namespace std {

#ifdef MLDB_STDLIB_GCC
template<class T, class A>
class vector;
#elif MLDB_STDLIB_LLVM
inline namespace __1 {
template<class T, class A>
class vector;
}
#else
#  error "Tell us how to forward declare set for your standard library"
#endif

}
