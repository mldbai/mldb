# This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.


$(eval $(call test,asio_timer_test,services,boost timed))

$(eval $(call test,epoll_test,services,boost))

$(eval $(call test,message_channel_test,services,boost))

$(eval $(call test,aws_test,cloud,boost))
#$(eval $(call test,hdfs_test,cloud,boost manual))

$(eval $(call test,statsd_connector_test,opstats,boost  manual))

$(eval $(call test,message_loop_test,services,boost))

$(eval $(call program,runner_test_helper,utils))
$(eval $(call test,runner_test,services,boost))
$(eval $(call test,runner_stress_test,services,boost manual))
$(TESTS)/runner_test $(TESTS)/runner_stress_test: $(BIN)/runner_test_helper
$(eval $(call test,sink_test,services,boost))
$(eval $(call program,async_writer_bench,services))

$(eval $(call test,sns_mock_test,cloud services,boost))


$(eval $(call test,fs_utils_test,cloud test_utils,boost manual))


