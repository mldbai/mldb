GXX_VERSION_MAJOR:=11
GCC?=gcc-11
GXX?=g++-11

CXX20_OPTION:=-std=c++20

include $(JML_BUILD)/gcc.mk

GCC_VERSION_WARNING_FLAGS:=-Wno-noexcept-type -Wno-overloaded-virtual -Wno-class-memaccess -Wno-array-bounds
