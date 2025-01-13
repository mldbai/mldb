# This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

$(eval $(call test,bitops_test,arch,boost))
$(eval $(call test,wait_on_address_test,arch,boost))
$(eval $(call test,fslock_test,arch,boost))
$(eval $(call test,simd_vector_test,arch,boost))
$(eval $(call test,simd_vector_benchmark,arch,boost manual))
$(eval $(call test,backtrace_test,arch,boost))
$(eval $(call test,bit_range_ops_test,arch,boost))
$(eval $(call test,info_test,arch,boost))
$(eval $(call test,rtti_utils_test,arch,boost))
$(eval $(call test,thread_specific_test,arch,boost))
$(eval $(call test,gc_test,gc arch,boost))
$(eval $(call test,shared_gc_lock_test,gc arch,boost manual)) # broken on some environments since gc lock changes
$(eval $(call test,rcu_protected_test,gc arch,boost timed))

ifeq ($(OSNAME),Linux)
$(eval $(call test,vm_test,arch,boost manual)) # latest linux path make this test fail https://lwn.net/Articles/642074/
endif

ifeq ($(ARCH),x86_64)
$(eval $(call test,sse2_math_test,arch,boost))
$(eval $(call test,simd_test,arch,boost))
$(eval $(call test,cpuid_test,arch,boost))
$(eval $(call test,tick_counter_test,arch,boost))
endif

ifeq ($(OSNAME)-$(ARCH),Darwin-arm64)
$(eval $(call test,tick_counter_test,arch,boost))
endif

$(eval $(call test,bit_array_test,arch,boost))
$(eval $(call test,bit_stream_writer_test,arch,catch2))
$(eval $(call test,bit_rank_select_test,arch,catch2))


ifeq ($(WITH_CUDA),1)
#$(eval $(call set_compile_option,cuda_device_query_test.cc,-I$(INC)))

#$(eval $(call test,cuda_device_query_test,cudart,plain))
#$(eval $(call test,cuda_init_test,arch_cuda,plain))
endif
