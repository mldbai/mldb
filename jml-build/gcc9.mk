GXX_VERSION_MAJOR:=9
GCC?=gcc-9
GXX?=g++-9

include $(JML_BUILD)/gcc.mk

GCC_VERSION_WARNING_FLAGS:=-Wno-noexcept-type
