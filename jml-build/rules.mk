# rules.mk
# Jeremy Barnes, 1 April 2006
# Copyright (c) 2006 Jeremy Barnes.  All rights reserved.
#
# $JML_LICENSE$
#

%/.dir_exists:
	@mkdir -p $(dir $@)
	@touch $@

all: test programs libraries compile tests

failed_tests: $(patsubst %.failed,%.passed,$(wildcard $(TESTS)/*.failed))

.PHONY: all test programs libraries compile tests failed_tests

