# This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

$(eval $(call test,asio_timer_test,io,boost))
$(eval $(call test,async_writer_bench,io,boost))
$(eval $(call test,epoll_test,io,boost))
$(eval $(call test,message_channel_test,io,boost))
$(eval $(call test,message_loop_test,io,boost))
