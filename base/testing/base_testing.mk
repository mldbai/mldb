# This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

export MLDB_NO_TIMING_TESTS=1
$(eval $(call test,thread_pool_test,base,boost timed))
$(eval $(call test,thread_queue_test,base,boost timed))
$(eval $(call test,per_thread_accumulator_test,base,boost))
