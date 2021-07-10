GXX_VERSION_MAJOR:=10
GCC?=gcc-10
GXX?=g++-10

include $(JML_BUILD)/gcc.mk

GCC_VERSION_WARNING_FLAGS:=-Wno-noexcept-type
