GXX_VERSION_MAJOR:=9
GCC?=gcc-9
GXX?=g++-9

CXX20_OPTION:=-std=c++2a

include $(JML_BUILD)/gcc.mk

GCC_VERSION_WARNING_FLAGS:=-Wno-noexcept-type -Wno-address-of-packed-member -Wno-class-memaccess -flax-vector-conversions
