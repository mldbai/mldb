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

LIBRUNNER_LINK := runner_common io_base value_description logging utils

$(eval $(call library,runner,$(LIBRUNNER_SOURCES),$(LIBRUNNER_LINK)))

$(LIB)/librunner.so: $(BIN)/runner_helper


# Services

LIBSERVICES_SOURCES := \
	event_service.cc \

LIBSERVICES_LINK := opstats

$(eval $(call library,services,$(LIBSERVICES_SOURCES),$(LIBSERVICES_LINK)))
$(eval $(call set_compile_option,runner.cc,-DBIN=\"$(BIN)\"))


# AWS

LIBAWS_SOURCES := \
	xml_helpers.cc \
	s3.cc \
	sns.cc \
	aws.cc \
	sqs.cc \

#	hdfs.cc

LIBAWS_LINK := credentials hash crypto++ tinyxml2


$(eval $(call library,aws,$(LIBAWS_SOURCES),$(LIBAWS_LINK)))

$(eval $(call program,sns_send,aws boost_program_options utils))

$(eval $(call include_sub_make,service_testing,testing,service_testing.mk))
