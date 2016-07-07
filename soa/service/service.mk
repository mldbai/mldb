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
	arch utils types

$(eval $(call library,opstats,$(LIBOPSTATS_SOURCES),$(LIBOPSTATS_LINK)))


# Runner Common

LIBRUNNERCOMMON_SOURCES := \
	runner_common.cc

LIBRUNNERCOMMON_LINK :=

$(eval $(call library,runner_common,$(LIBRUNNERCOMMON_SOURCES),$(LIBRUNNERCOMMON_LINK)))
$(eval $(call program,runner_helper,runner_common arch))


# Runner

LIBRUNNER_SOURCES := \
	sink.cc \
	runner.cc

LIBRUNNER_LINK := runner_common io value_description logging utils

$(eval $(call library,runner,$(LIBRUNNER_SOURCES),$(LIBRUNNER_LINK)))

$(LIB)/librunner.so: $(BIN)/runner_helper


# Services

LIBSERVICES_SOURCES := \
	event_service.cc \

LIBSERVICES_LINK := opstats

$(eval $(call library,services,$(LIBSERVICES_SOURCES),$(LIBSERVICES_LINK)))
$(eval $(call set_compile_option,runner.cc,-DBIN=\"$(BIN)\"))

LIBCLOUD_SOURCES := \
	xml_helpers.cc \
	sftp.cc \
	s3.cc \
	sns.cc \
	aws.cc \
	sqs.cc \
	archive.cc \
	docker.cc

#	hdfs.cc

LIBCLOUD_LINK := credentials utils arch types value_description tinyxml2 crypto++ ssh2 boost_filesystem archive hash #hdfs3


$(eval $(call library,cloud,$(LIBCLOUD_SOURCES),$(LIBCLOUD_LINK)))

# gcc 4.7
$(eval $(call set_compile_option,aws.cc,-fpermissive))

$(eval $(call program,s3_transfer_cmd,cloud boost_program_options boost_filesystem utils))
$(eval $(call program,s3tee,cloud boost_program_options utils))
$(eval $(call program,s3cp,cloud boost_program_options utils))
$(eval $(call program,s3_multipart_cmd,cloud boost_program_options utils))
$(eval $(call program,s3cat,cloud boost_program_options utils))
$(eval $(call program,sns_send,cloud boost_program_options utils))

$(eval $(call include_sub_make,service_testing,testing,service_testing.mk))
