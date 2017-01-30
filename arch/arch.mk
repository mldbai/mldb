# This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

LIBARCH_SOURCES := \
        simd_vector.cc \
        demangle.cc \
	tick_counter.cc \
	cpuid.cc \
	simd.cc \
	exception.cc \
	exception_handler.cc \
	backtrace.cc \
	format.cc \
	fslock.cc \
	gpgpu.cc \
	environment_static.cc \
	cpu_info.cc \
	vm.cc \
	info.cc \
	rtti_utils.cc \
	rt.cc \
	abort.cc \
	spinlock.cc \

ifeq ($(ARCH),x86_64)
LIBARCH_SOURCES += simd_vector_avx.cc
endif

LIBARCH_LINK := dl

ifneq ($(BOOST_VERSION),42)
LIBARCH_LINK += boost_system
endif

$(eval $(call library,arch,$(LIBARCH_SOURCES),$(LIBARCH_LINK)))

LIBGC_SOURCES := \
	gc_lock.cc \
	shared_gc_lock.cc

$(eval $(call library,gc,$(LIBGC_SOURCES),rt arch))

# Note: we should be able to get away without this, but we get a segfault on
# shared library loading if it's not here.
$(eval $(call set_single_compile_option,simd_vector_avx.cc,-mavx))

$(eval $(call library,exception_hook,exception_hook.cc,arch dl))

$(eval $(call library,node_exception_tracing,node_exception_tracing.cc,exception_hook arch dl))


ifeq ($(CAL_ENABLED),1)

LIBARCH_CAL_SOURCES 	:= cal.cc
LIBARCH_CAL_LINK 	:= arch amd

$(eval $(call library,arch_cal,$(LIBARCH_CAL_SOURCES),$(LIBARCH_CAL_LINK)))

endif # CAL_ENABLED

$(eval $(call include_sub_make,arch_testing,testing))
