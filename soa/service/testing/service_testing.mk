# This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

$(eval $(call test,statsd_connector_test,opstats,boost  manual))

$(eval $(call test,aws_test,aws,boost))
$(eval $(call test,sns_mock_test,aws services,boost))

$(eval $(call program,runner_test_helper,utils))
$(eval $(call test,runner_test,runner,boost))
$(eval $(call test,runner_stress_test,runner,boost manual))
$(TESTS)/runner_test $(TESTS)/runner_stress_test: $(BIN)/runner_test_helper
$(eval $(call test,sink_test,runner utils,boost))
