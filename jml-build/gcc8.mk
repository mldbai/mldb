GCC?=gcc-8
GXX?=g++-8

include $(JML_BUILD)/gcc.mk

GCC_VERSION_WARNING_FLAGS:=-Wno-noexcept-type
GXX_VERSION_MAJOR:=8
