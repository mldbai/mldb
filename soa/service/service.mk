# This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

# services makefile
# Jeremy Barnes, 29 May 2012


LIBOPSTATS_SOURCES := \
	multi_aggregator.cc \
	statsd_connector.cc \
	stat_aggregator.cc \
	process_stats.cc \
	connectfd.cc

LIBOPSTATS_LINK := \
	arch utils boost_thread types

$(eval $(call library,opstats,$(LIBOPSTATS_SOURCES),$(LIBOPSTATS_LINK)))



LIBDATACRATIC_RUNNERCOMMON_SOURCES := \
	runner_common.cc

LIBDATACRATIC_RUNNERCOMMON_LINK :=

$(eval $(call library,runner_common,$(LIBDATACRATIC_RUNNERCOMMON_SOURCES),$(LIBDATACRATIC_RUNNERCOMMON_LINK)))
$(eval $(call program,runner_helper,runner_common arch))


LIBSERVICES_SOURCES := \
	epoller.cc \
	epoll_loop.cc \
	event_service.cc \
	message_loop.cc \
	async_event_source.cc \
	async_writer_source.cc \
	sink.cc \
	xml_helpers.cc \
	remote_credential_provider.cc \
	runner.cc \


LIBSERVICES_LINK := http opstats curl boost_regex arch utils jsoncpp boost_thread types tinyxml2 boost_system value_description credentials runner_common boost_filesystem cityhash any services_base watch

$(eval $(call library,services,$(LIBSERVICES_SOURCES),$(LIBSERVICES_LINK)))
$(eval $(call set_compile_option,runner.cc,-DBIN=\"$(BIN)\"))

# gcc 4.7
$(eval $(call set_compile_option,aws.cc,-fpermissive))

$(LIB)/libservices.so: $(BIN)/runner_helper



LIBCLOUD_SOURCES := \
	sftp.cc \
	s3.cc \
	sns.cc \
	aws.cc \
	sqs.cc \
	archive.cc \
	docker.cc

#	hdfs.cc

LIBCLOUD_LINK := utils arch types value_description tinyxml2 services crypto++ ssh2 boost_filesystem archive #hdfs3


$(eval $(call library,cloud,$(LIBCLOUD_SOURCES),$(LIBCLOUD_LINK)))



$(eval $(call program,s3_transfer_cmd,cloud boost_program_options boost_filesystem utils))
$(eval $(call program,s3tee,cloud boost_program_options utils))
$(eval $(call program,s3cp,cloud boost_program_options utils))
$(eval $(call program,s3_multipart_cmd,cloud boost_program_options utils))
$(eval $(call program,s3cat,cloud boost_program_options utils))
$(eval $(call program,sns_send,cloud boost_program_options utils))

SERVICEDUMP_LINK = services boost_program_options


$(eval $(call include_sub_make,service_testing,testing,service_testing.mk))
