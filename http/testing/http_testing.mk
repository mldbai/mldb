# This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

$(eval $(call test,http_header_test,http,boost manual))
$(eval $(call test,MLDB-1016-http-connection-overflow,http,boost))
$(eval $(call test,http_parsers_test,http,boost valgrind))
$(eval $(call test,tcp_acceptor_test+http,http,boost))
$(eval $(call test,tcp_acceptor_threaded_test+http,http,boost))
$(eval $(call program,http_service_bench,boost_program_options http))
$(eval $(call library,test_services,test_http_services.cc,http io_base))
$(eval $(call program,http_client_bench,boost_program_options http test_services value_description))
$(eval $(call test,http_client_test,http test_services,boost))
$(eval $(call test,http_client_online_test,http io_base,boost manual))

# The following tests needs to be adapted to the new network code:
# $(eval $(call test,endpoint_unit_test,http,boost))
# $(eval $(call test,test_active_endpoint_nothing_listening,http,boost manual))
# $(eval $(call test,test_active_endpoint_not_responding,http,boost manual))
# $(eval $(call test,test_endpoint_ping_pong,http,boost manual))
# $(eval $(call test,test_endpoint_connection_speed,http,boost manual))
# $(eval $(call test,test_endpoint_accept_speed,http,boost))
# $(eval $(call test,endpoint_closed_connection_test,http,boost))
# $(eval $(call test,http_long_header_test,http,boost manual))
