GXX_VERSION_MAJOR:=14
GCC?=gcc-14
GXX?=g++-14

CXX20_OPTION:=-std=c++20

include $(JML_BUILD)/gcc.mk

GCC_VERSION_WARNING_FLAGS:=-Wno-noexcept-type -Wno-overloaded-virtual -Wno-class-memaccess -Wno-array-bounds -Wno-template-id-cdtor -Wno-dangling-pointer -Wno-dangling-reference -Wno-free-nonheap-object -fdiagnostics-color=always
