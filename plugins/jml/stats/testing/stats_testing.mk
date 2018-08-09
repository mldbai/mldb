# This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

# stats_testing.mk
# Jeremy Barnes, 9 November 2009
# Copyright (c) 2009 Jeremy Barnes.  All rights reserved.
#
# Testing for stats functionality.

$(eval $(call test,rmse_test,stats arch,boost))

