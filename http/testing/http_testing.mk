# This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

HTTP_TESTING_EXTRA_LIBS:=arch types value_description io_base

$(eval $(call test,http_header_test,http $(HTTP_TESTING_EXTRA_LIBS),boost manual))
$(eval $(call test,http_parsers_test,http $(HTTP_TESTING_EXTRA_LIBS),boost valgrind))
$(eval $(call test,tcp_acceptor_test+http,http $(HTTP_TESTING_EXTRA_LIBS),boost))
$(eval $(call test,tcp_acceptor_threaded_test+http,http $(HTTP_TESTING_EXTRA_LIBS),boost))
$(eval $(call program,http_service_bench,boost_program_options http $(HTTP_TESTING_EXTRA_LIBS)))
$(eval $(call library,test_services,test_http_services.cc,http $(HTTP_TESTING_EXTRA_LIBS)))
$(eval $(call program,http_client_bench,boost_program_options http test_services $(HTTP_TESTING_EXTRA_LIBS)))
#$(eval $(call test,http_client_test,http test_services $(HTTP_TESTING_EXTRA_LIBS),boost)) # manual: failing
$(eval $(call test,http_client_test2,http test_services $(HTTP_TESTING_EXTRA_LIBS),boost))
$(eval $(call test,http_client_stress_test,http test_services $(HTTP_TESTING_EXTRA_LIBS),boost manual))  # manual: too long
$(eval $(call test,http_client_online_test,http $(HTTP_TESTING_EXTRA_LIBS),boost manual))

# The following tests needs to be adapted to the new network code:
# $(eval $(call test,endpoint_unit_test,http,boost))
# $(eval $(call test,test_active_endpoint_nothing_listening,http,boost manual))
# $(eval $(call test,test_active_endpoint_not_responding,http,boost manual))
# $(eval $(call test,test_endpoint_ping_pong,http,boost manual))
# $(eval $(call test,test_endpoint_connection_speed,http,boost manual))
# $(eval $(call test,test_endpoint_accept_speed,http,boost))
# $(eval $(call test,endpoint_closed_connection_test,http,boost))
# $(eval $(call test,http_long_header_test,http,boost manual))
