GXX_VERSION_MAJOR:=11
GCC?=gcc-11
GXX?=g++-11

include $(JML_BUILD)/gcc.mk

GCC_VERSION_WARNING_FLAGS:=-Wno-noexcept-type
