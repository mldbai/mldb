GCC?=gcc-5
GXX?=g++-5

GCC_VERSION_WARNING_FLAGS:=-Wno-unused-local-typedefs

include $(JML_BUILD)/gcc.mk

