#
# casablanca.mk
# Mich, 2017-01-13
# This file is part of MLDB. Copyright 2017 mldb.ai inc. All rights reserved.
#
# Builds casablanca, testrunner and some of casablanca tests. Files commented
# out are not built.
#

CASABLANCA_SOURCE := \
./Release/src/uri/uri_parser.cpp \
./Release/src/uri/uri.cpp \
./Release/src/uri/uri_builder.cpp \
./Release/src/utilities/base64.cpp \
./Release/src/utilities/asyncrt_utils.cpp \
./Release/src/utilities/web_utilities.cpp \
./Release/src/http/oauth/oauth1.cpp \
./Release/src/http/oauth/oauth2.cpp \
./Release/src/http/listener/http_listener_msg.cpp \
./Release/src/http/listener/http_server_api.cpp \
./Release/src/http/listener/http_listener.cpp \
./Release/src/http/listener/http_server_asio.cpp \
./Release/src/http/common/http_msg.cpp \
./Release/src/http/common/http_helpers.cpp \
./Release/src/http/client/http_client_msg.cpp \
./Release/src/http/client/http_client.cpp \
./Release/src/http/client/http_client_asio.cpp \
./Release/src/http/client/x509_cert_utilities.cpp \
./Release/src/streams/fileio_posix.cpp \
./Release/src/pch/stdafx.cpp \
./Release/src/json/json.cpp \
./Release/src/json/json_parsing.cpp \
./Release/src/json/json_serialization.cpp \

#./Release/libs/websocketpp/tutorials/utility_client/step5.cpp \
./Release/libs/websocketpp/tutorials/utility_client/step4.cpp \
./Release/libs/websocketpp/tutorials/utility_client/step3.cpp \
./Release/libs/websocketpp/tutorials/utility_client/step1.cpp \
./Release/libs/websocketpp/tutorials/utility_client/step2.cpp \
./Release/libs/websocketpp/tutorials/utility_client/step6.cpp \
./Release/libs/websocketpp/docs/simple_count_server_thread.cpp \
./Release/libs/websocketpp/docs/simple_broadcast_server.cpp \
./Release/libs/websocketpp/examples/handler_switch/handler_switch.cpp \
./Release/libs/websocketpp/examples/iostream_server/iostream_server.cpp \
./Release/libs/websocketpp/examples/simple_broadcast_server/simple_broadcast_server.cpp \
./Release/libs/websocketpp/examples/debug_client/debug_client.cpp \
./Release/libs/websocketpp/examples/echo_server_both/echo_server_both.cpp \
./Release/libs/websocketpp/examples/subprotocol_server/subprotocol_server.cpp \
./Release/libs/websocketpp/examples/dev/main.cpp \
./Release/libs/websocketpp/examples/broadcast_server/broadcast_server.cpp \
./Release/libs/websocketpp/examples/echo_server_tls/echo_server_tls.cpp \
./Release/libs/websocketpp/examples/print_server/print_server.cpp \
./Release/libs/websocketpp/examples/associative_storage/associative_storage.cpp \
./Release/libs/websocketpp/examples/telemetry_server/telemetry_server.cpp \
./Release/libs/websocketpp/examples/testee_client/testee_client.cpp \
./Release/libs/websocketpp/examples/testee_server/testee_server.cpp \
./Release/libs/websocketpp/examples/telemetry_client/telemetry_client.cpp \
./Release/libs/websocketpp/examples/debug_server/debug_server.cpp \
./Release/libs/websocketpp/examples/utility_client/utility_client.cpp \
./Release/libs/websocketpp/examples/enriched_storage/enriched_storage.cpp \
./Release/libs/websocketpp/examples/echo_server/echo_server.cpp \
./Release/libs/websocketpp/examples/sip_client/sip_client.cpp \
./Release/libs/websocketpp/test/connection/connection.cpp \
./Release/libs/websocketpp/test/connection/connection_tu2.cpp \
./Release/libs/websocketpp/test/logger/basic.cpp \
./Release/libs/websocketpp/test/endpoint/endpoint.cpp \
./Release/libs/websocketpp/test/utility/close.cpp \
./Release/libs/websocketpp/test/utility/error.cpp \
./Release/libs/websocketpp/test/utility/uri.cpp \
./Release/libs/websocketpp/test/utility/utilities.cpp \
./Release/libs/websocketpp/test/utility/frame.cpp \
./Release/libs/websocketpp/test/utility/sha1.cpp \
./Release/libs/websocketpp/test/random/none.cpp \
./Release/libs/websocketpp/test/random/random_device.cpp \
./Release/libs/websocketpp/test/processors/extension_permessage_compress.cpp \
./Release/libs/websocketpp/test/processors/hybi08.cpp \
./Release/libs/websocketpp/test/processors/hybi13.cpp \
./Release/libs/websocketpp/test/processors/hybi00.cpp \
./Release/libs/websocketpp/test/processors/processor.cpp \
./Release/libs/websocketpp/test/processors/hybi07.cpp \
./Release/libs/websocketpp/test/extension/permessage_deflate.cpp \
./Release/libs/websocketpp/test/extension/extension.cpp \
./Release/libs/websocketpp/test/http/parser_perf.cpp \
./Release/libs/websocketpp/test/http/parser.cpp \
./Release/libs/websocketpp/test/transport/hybi_util.cpp \
./Release/libs/websocketpp/test/transport/iostream/connection.cpp \
./Release/libs/websocketpp/test/transport/iostream/base.cpp \
./Release/libs/websocketpp/test/transport/iostream/endpoint.cpp \
./Release/libs/websocketpp/test/transport/integration.cpp \
./Release/libs/websocketpp/test/transport/asio/timers.cpp \
./Release/libs/websocketpp/test/transport/asio/base.cpp \
./Release/libs/websocketpp/test/message_buffer/alloc.cpp \
./Release/libs/websocketpp/test/message_buffer/message.cpp \
./Release/libs/websocketpp/test/message_buffer/pool.cpp \
./Release/libs/websocketpp/test/roles/client.cpp \
./Release/libs/websocketpp/test/roles/server.cpp

# ./Release/src/http/listener/http_server_httpsys.cpp \

#./Release/src/pplx/pplxapple.cpp \

#./Release/src/pplx/pplxwin.cpp \
./Release/src/http/client/http_client_winhttp.cpp \
./Release/src/http/client/http_client_winrt.cpp \
./Release/src/streams/fileio_win32.cpp \
./Release/src/streams/fileio_winrt.cpp \

#./Release/src/websockets/client/ws_client.cpp \
./Release/src/websockets/client/ws_client_wspp.cpp \
./Release/src/websockets/client/ws_client_winrt.cpp \
./Release/src/websockets/client/ws_msg.cpp \

#./Release/tests/functional/websockets/utilities/test_websocket_server.cpp \
./Release/tests/functional/websockets/utilities/stdafx.cpp \
./Release/tests/functional/websockets/client/receive_msg_tests.cpp \
./Release/tests/functional/websockets/client/send_msg_tests.cpp \
./Release/tests/functional/websockets/client/close_tests.cpp \
./Release/tests/functional/websockets/client/client_construction.cpp \
./Release/tests/functional/websockets/client/proxy_tests.cpp \
./Release/tests/functional/websockets/client/authentication_tests.cpp \
./Release/tests/functional/websockets/client/error_tests.cpp \
./Release/tests/functional/websockets/client/stdafx.cpp \
./Release/tests/functional/misc/atl_headers/header_test2.cpp \
./Release/tests/functional/misc/atl_headers/header_test1.cpp \
./Release/tests/functional/misc/version/version.cpp \
./Release/tests/functional/uri/encoding_tests.cpp \
./Release/tests/functional/uri/splitting_tests.cpp \
./Release/tests/functional/uri/accessor_tests.cpp \
./Release/tests/functional/uri/uri_builder_tests.cpp \
./Release/tests/functional/uri/diagnostic_tests.cpp \
./Release/tests/functional/uri/combining_tests.cpp \
./Release/tests/functional/uri/constructor_tests.cpp \
./Release/tests/functional/uri/stdafx.cpp \
./Release/tests/functional/uri/conversions_tests.cpp \
./Release/tests/functional/uri/operator_tests.cpp \
./Release/tests/functional/http/utilities/test_http_client.cpp \
./Release/tests/functional/http/utilities/http_asserts.cpp \
./Release/tests/functional/http/utilities/stdafx.cpp \
./Release/tests/functional/http/utilities/test_http_server.cpp \
./Release/tests/functional/http/utilities/test_server_utilities.cpp \
./Release/tests/functional/http/listener/building_response_tests.cpp \
./Release/tests/functional/http/listener/request_extract_tests.cpp \
./Release/tests/functional/http/listener/listener_construction_tests.cpp \
./Release/tests/functional/http/listener/reply_helper_tests.cpp \
./Release/tests/functional/http/listener/request_relative_uri_tests.cpp \
./Release/tests/functional/http/listener/header_tests.cpp \
./Release/tests/functional/http/listener/requests_tests.cpp \
./Release/tests/functional/http/listener/connections_and_errors.cpp \
./Release/tests/functional/http/listener/request_stream_tests.cpp \
./Release/tests/functional/http/listener/to_string_tests.cpp \
./Release/tests/functional/http/listener/status_code_reason_phrase_tests.cpp \
./Release/tests/functional/http/listener/stdafx.cpp \
./Release/tests/functional/http/listener/request_handler_tests.cpp \
./Release/tests/functional/http/listener/response_stream_tests.cpp \
./Release/tests/functional/http/client/multiple_requests.cpp \
./Release/tests/functional/http/client/outside_tests.cpp \
./Release/tests/functional/http/client/request_uri_tests.cpp \
./Release/tests/functional/http/client/http_methods_tests.cpp \
./Release/tests/functional/http/client/pipeline_stage_tests.cpp \
./Release/tests/functional/http/client/header_tests.cpp \
./Release/tests/functional/http/client/connections_and_errors.cpp \
./Release/tests/functional/http/client/request_stream_tests.cpp \
./Release/tests/functional/http/client/request_helper_tests.cpp \
./Release/tests/functional/http/client/to_string_tests.cpp \
./Release/tests/functional/http/client/progress_handler_tests.cpp \
./Release/tests/functional/http/client/oauth1_tests.cpp \
./Release/tests/functional/http/client/client_construction.cpp \
./Release/tests/functional/http/client/proxy_tests.cpp \
./Release/tests/functional/http/client/authentication_tests.cpp \
./Release/tests/functional/http/client/oauth2_tests.cpp \
./Release/tests/functional/http/client/http_client_tests.cpp \
./Release/tests/functional/http/client/http_client_fuzz_tests.cpp \
./Release/tests/functional/http/client/response_extract_tests.cpp \
./Release/tests/functional/http/client/status_code_reason_phrase_tests.cpp \
./Release/tests/functional/http/client/stdafx.cpp \
./Release/tests/functional/http/client/building_request_tests.cpp \
./Release/tests/functional/http/client/response_stream_tests.cpp \
./Release/tests/functional/streams/winrt_interop_tests.cpp \
./Release/tests/functional/streams/istream_tests.cpp \
./Release/tests/functional/streams/CppSparseFile.cpp \
./Release/tests/functional/streams/fstreambuf_tests.cpp \
./Release/tests/functional/streams/ostream_tests.cpp \
./Release/tests/functional/streams/fuzz_tests.cpp \
./Release/tests/functional/streams/stdafx.cpp \
./Release/tests/functional/streams/memstream_tests.cpp \
./Release/tests/functional/streams/stdstream_tests.cpp \
./Release/tests/functional/utils/base64.cpp \
./Release/tests/functional/utils/nonce_generator_tests.cpp \
./Release/tests/functional/utils/macro_test.cpp \
./Release/tests/functional/utils/stdafx.cpp \
./Release/tests/functional/utils/strings.cpp \
./Release/tests/functional/utils/datetime.cpp \
./Release/tests/functional/pplx/pplx_test/pplxtask_tests.cpp \
./Release/tests/functional/pplx/pplx_test/pplx_op_test.cpp \
./Release/tests/functional/pplx/pplx_test/stdafx.cpp \
./Release/tests/functional/pplx/pplx_test/pplx_task_options.cpp \
./Release/tests/functional/json/iterator_tests.cpp \
./Release/tests/functional/json/json_numbers_tests.cpp \
./Release/tests/functional/json/parsing_tests.cpp \
./Release/tests/functional/json/construction_tests.cpp \
./Release/tests/functional/json/fuzz_tests.cpp \
./Release/tests/functional/json/stdafx.cpp \
./Release/tests/functional/json/to_as_and_operators_tests.cpp \
./Release/tests/functional/json/negative_parsing_tests.cpp \
./Release/tests/common/utilities/os_utilities.cpp \
./Release/tests/common/utilities/stdafx.cpp \
./Release/tests/common/TestRunner/test_runner.cpp \
./Release/tests/common/TestRunner/vs14.android/TestRunner.android.NativeActivity/main.cpp \

#./Release/samples/FacebookDemo/MainPage.xaml.cpp \
./Release/samples/FacebookDemo/Facebook.cpp \
./Release/samples/FacebookDemo/App.xaml.cpp \
./Release/samples/FacebookDemo/pch.cpp \
./Release/samples/SearchFile/searchfile.cpp \
./Release/samples/BingRequest/bingrequest.cpp \
./Release/samples/CasaLens/datafetcher.cpp \
./Release/samples/CasaLens/stdafx.cpp \
./Release/samples/CasaLens/casalens.cpp \
./Release/samples/WindowsLiveAuth/MainPage.xaml.cpp \
./Release/samples/WindowsLiveAuth/App.xaml.cpp \
./Release/samples/WindowsLiveAuth/pch.cpp \
./Release/samples/Oauth2Client/Oauth2Client.cpp \
./Release/samples/Oauth2Client/stdafx.cpp \
./Release/samples/OAuth2Live/MainPage.xaml.cpp \
./Release/samples/OAuth2Live/App.xaml.cpp \
./Release/samples/OAuth2Live/pch.cpp \
./Release/samples/Oauth1Client/stdafx.cpp \
./Release/samples/Oauth1Client/Oauth1Client.cpp \
./Release/samples/BlackJack/BlackJack_UIClient/PlayingTable.xaml.cpp \
./Release/samples/BlackJack/BlackJack_UIClient/Player.xaml.cpp \
./Release/samples/BlackJack/BlackJack_UIClient/App.xaml.cpp \
./Release/samples/BlackJack/BlackJack_UIClient/CardShape.xaml.cpp \
./Release/samples/BlackJack/BlackJack_UIClient/pch.cpp \
./Release/samples/BlackJack/BlackJack_Server/Table.cpp \
./Release/samples/BlackJack/BlackJack_Server/BlackJack_Server.cpp \
./Release/samples/BlackJack/BlackJack_Server/Dealer.cpp \
./Release/samples/BlackJack/BlackJack_Server/stdafx.cpp \
./Release/samples/BlackJack/BlackJack_Client/BlackJackClient.cpp \
./Release/samples/BlackJack/BlackJack_Client/stdafx.cpp \

CASABLANCA_FLAGS := \
    -D_DEBUG \
    -Imldb/ext/casablanca/Release/src/pch \
    -Imldb/ext/casablanca/Release/include \
    -Imldb/ext/casablanca/Release/libs \
    -Wno-overloaded-virtual \

$(eval $(call set_compile_option,$(CASABLANCA_SOURCE),$(CASABLANCA_FLAGS)))
$(eval $(call library,casablanca,$(CASABLANCA_SOURCE),ssl))

PPLX_SOURCE := \
./Release/src/pplx/pplxlinux.cpp \
./Release/src/pplx/pplx.cpp \
./Release/src/pplx/threadpool.cpp \

PPLX_FLAGS := \
    -D_DEBUG \
    -Imldb/ext/casablanca/Release/src/pch \
    -Imldb/ext/casablanca/Release/include \
    -Imldb/ext/casablanca/Release/libs \
    -Wno-overloaded-virtual \


$(eval $(call set_compile_option,$(PPLX_SOURCE),$(PPLX_FLAGS)))
$(eval $(call library,pplx,$(PPLX_SOURCE)))

CASABLANCA_UNITTESTPP_SOURCE := \
    $(shell cd mldb/ext/casablanca && ls Release/tests/common/UnitTestpp/src/*.cpp) \
    $(shell cd mldb/ext/casablanca && ls Release/tests/common/UnitTestpp/src/Posix/*.cpp) \


#./Release/tests/common/UnitTestpp/src/Win32/TimeHelpers.cpp \
./Release/tests/common/UnitTestpp/src/tests/TestXmlTestReporter.cpp \
./Release/tests/common/UnitTestpp/src/tests/TestDeferredTestReporter.cpp \
./Release/tests/common/UnitTestpp/src/tests/TestUnitTestPP.cpp \
./Release/tests/common/UnitTestpp/src/tests/TestTestList.cpp \
./Release/tests/common/UnitTestpp/src/tests/TestCurrentTest.cpp \
./Release/tests/common/UnitTestpp/src/tests/TestCheckMacros.cpp \
./Release/tests/common/UnitTestpp/src/tests/TestTestMacros.cpp \
./Release/tests/common/UnitTestpp/src/tests/TestTest.cpp \
./Release/tests/common/UnitTestpp/src/tests/TestCompositeTestReporter.cpp \
./Release/tests/common/UnitTestpp/src/tests/TestChecks.cpp \
./Release/tests/common/UnitTestpp/src/tests/TestTestResults.cpp \
./Release/tests/common/UnitTestpp/src/tests/TestMemoryOutStream.cpp \
./Release/tests/common/UnitTestpp/src/tests/TestTestSuite.cpp \
./Release/tests/common/UnitTestpp/src/tests/stdafx.cpp \
./Release/tests/common/UnitTestpp/src/tests/TestAssertHandler.cpp \
./Release/tests/common/UnitTestpp/src/tests/TestTestRunner.cpp \

CASABLANCA_UNITTESTPP_FLAGS := \
    -D_DEBUG \
    -Imldb/ext/casablanca/Release/src/pch \
    -Imldb/ext/casablanca/Release/include \
    -Imldb/ext/casablanca/Release/libs \
    -Wno-overloaded-virtual \

$(eval $(call set_compile_option,$(CASABLANCA_UNITTESTPP_SOURCE),$(CASABLANCA_UNITTESTPP_FLAGS)))
$(eval $(call library,unittestpp,$(CASABLANCA_UNITTESTPP_SOURCE)))


# $(1) filepath
define casablanca_test
ifneq ($(PREMAKE),1)
$(TESTS)/casablanca/$(1).passed: $(BIN)/test_runner $(TESTS)/casablanca/$(1).so
	@(cd $(TESTS)/casablanca && ../../bin/test_runner $(1).so > $(1).log 2>&1 && cd $(PWD) && touch $$@ && echo "                       $(COLOR_GREEN)casablanca $(1) passed$(COLOR_RESET)") || (echo "                       $(COLOR_RED)casablanca $(1) FAILED$(COLOR_RESET)" && exit 1)

autotest: $(TESTS)/casablanca/$(1).passed

casablanca_tests: $(TESTS)/casablanca/$(1).passed

endif
endef

#$(eval $(call casablanca_test,encoding_tests))


ifneq ($(PREMAKE),1)
# some depend on windows, on unknown header, or have non unique filenames,
# which causes problems with jml-build
CASABLANCA_TEST_FILES_FILTER_OUT := \
    %/stdafx.cpp \
    %/header_test1.cpp \
    %/header_test2.cpp \
    %/version.cpp \
    %/winrt_interop_tests.cpp \
    %/CppSparseFile.cpp \
    $(addprefix %/,$(shell cd mldb/ext/casablanca/Release/tests/functional && find . -name "*.cpp" | rev | cut -d / -f 1 | rev | sort | uniq -d)) \

CASABLANCA_TEST_FILES := $(patsubst ./%,functional/%,$(filter-out $(CASABLANCA_TEST_FILES_FILTER_OUT),$(shell cd mldb/ext/casablanca/Release/tests/functional && find . -name "*.cpp")))

CASABLANCA_TEST_FLAGS := \
    $(CASABLANCA_FLAGS) \
    -Imldb/ext/casablanca/Release/tests/common/UnitTestpp \
    -Imldb/ext/casablanca/Release/tests/common/utilities/include \
    -Imldb/ext/casablanca/Release/tests/functional/http/utilities/include \
    -Imldb/ext/casablanca/Release/include/cpprest \
    -Imldb/ext/casablanca/Release/tests/functional/websockets/utilities \
    -Imldb/ext/casablanca/Release/libs/websocketpp \
    -llibunittest++-dev \
    -Wno-delete-non-virtual-dtor \

$(foreach file,$(CASABLANCA_TEST_FILES),$(eval $(call set_compile_option,Release/tests/$(file),$(CASABLANCA_TEST_FLAGS))))
$(foreach file,$(CASABLANCA_TEST_FILES),$(eval $(call library,$(basename $(notdir $(file))),Release/tests/$(file),casablanca pplx unittestpp,$(basename $(notdir $(file))),,,$(TESTS)/casablanca/$(patsubst %/,%,$(dir $(file))))))
$(foreach file,$(CASABLANCA_TEST_FILES),$(eval $(call casablanca_test,$(basename $(file)))))

endif

TEST_RUNNER_SOURCE := \
./Release/tests/common/TestRunner/test_runner.cpp \
./Release/tests/common/TestRunner/test_module_loader.cpp \

TEST_RUNNER_FLAGS := \
    -D_DEBUG \
    -Imldb/ext/casablanca/Release/src/pch \
    -Imldb/ext/casablanca/Release/include \
    -Imldb/ext/casablanca/Release/libs \
    -Imldb/ext/casablanca/Release/tests/common/UnitTestpp \
    -Wno-overloaded-virtual \
    -Wno-reorder \

$(eval $(call set_compile_option,$(TEST_RUNNER_SOURCE),$(TEST_RUNNER_FLAGS)))
$(eval $(call program,test_runner,boost_filesystem unittestpp,$(TEST_RUNNER_SOURCE)))
