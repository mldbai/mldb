# This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

$(eval $(call test,asio_timer_test,io_base arch base value_description watch types,boost))
$(eval $(call test,async_writer_bench,io_base arch types,boost manual))
$(eval $(call test,epoll_test,io_base arch base,boost))
$(eval $(call test,epoller_test,io_base,boost))
$(eval $(call test,timerfd_test,io_base,boost))
$(eval $(call test,message_channel_test,io_base arch types,boost))
$(eval $(call test,message_loop_test,io_base arch types,boost))
