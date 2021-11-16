#include <typeinfo>
#include <iostream>

// Sometimes the typeinfo node for _Float16 is missing (llvm on OSX, at least)
// Create it here if that's the case
struct HalfTypeInfo: public std::type_info {
    HalfTypeInfo(): std::type_info("Dh") {};
};

HalfTypeInfo _ZTIDF16_ __attribute__((__weak__));

