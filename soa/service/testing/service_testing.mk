# This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

$(eval $(call test,aws_test,cloud,boost))
#$(eval $(call test,hdfs_test,cloud,boost manual))

$(eval $(call test,statsd_connector_test,opstats,boost  manual))

$(eval $(call program,runner_test_helper,utils))
$(eval $(call test,runner_test,runner,boost))
$(eval $(call test,runner_stress_test,runner,boost manual))
$(TESTS)/runner_test $(TESTS)/runner_stress_test: $(BIN)/runner_test_helper
$(eval $(call test,sink_test,runner utils,boost))

$(eval $(call test,sns_mock_test,cloud services,boost))


$(eval $(call test,fs_utils_test,cloud test_utils,boost manual))
