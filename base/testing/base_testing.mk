# This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

export MLDB_NO_TIMING_TESTS=1
$(eval $(call test,thread_pool_test,base arch,boost timed))
$(eval $(call test,thread_queue_test,base arch,boost timed))
$(eval $(call test,per_thread_accumulator_test,base arch,boost))
$(eval $(call test,iostream_adaptors_test,arch base, catch2))
$(eval $(call test,processing_state_test,arch base,catch2))
