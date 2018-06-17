GCC?=gcc-7
GXX?=g++-7

include $(JML_BUILD)/gcc.mk

GCC_VERSION_WARNING_FLAGS:=-Wno-noexcept-type
GXX_VERSION_MAJOR:=7
