GXX_VERSION_MAJOR:=13
GCC?=gcc-13
GXX?=g++-13

CXX20_OPTION:=-std=c++20

include $(JML_BUILD)/gcc.mk

GCC_VERSION_WARNING_FLAGS:=-Wno-noexcept-type -Wno-overloaded-virtual -Wno-class-memaccess -Wno-array-bounds -Wno-template-id-cdtor -Wno-dangling-pointer -fdiagnostics-color=always
