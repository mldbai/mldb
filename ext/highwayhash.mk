
HIGHWAYHASH_SOURCE_x86_64 := hh_avx2.cc hh_sse41.cc
HIGHWAYHASH_SOURCE_arm64 := hh_neon.cc
HIGHWAYHASH_SOURCE_aarch64 := $(HIGHWAYHASH_SOURCE_arm64)

HIGHWAYHASH_SOURCE = ../highwayhash.cc $(addprefix highwayhash/,sip_hash.cc hh_portable.cc arch_specific.cc instruction_sets.cc nanobenchmark.cc os_specific.cc c_bindings.cc $(HIGHWAYHASH_SOURCE_$(ARCH)))
$(eval $(call set_compile_option,$(HIGHWAYHASH_SOURCE),-I$(CWD) -Wno-unused-private-field))

ifeq ($(ARCH),x86_64)
$(eval $(call set_compile_option,highwayhash/hh_sse41.cc,-msse4.1 -I$(CWD)))
$(eval $(call set_compile_option,highwayhash/hh_avx2.cc,-mavx2 -I$(CWD)))
endif

$(eval $(call library,highwayhash,$(HIGHWAYHASH_SOURCE),))
