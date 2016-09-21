LIBMLDB_CORE_SOURCES:= \
	plugin.cc \
	dataset.cc \
	procedure.cc \
	recorder.cc \
	function.cc \
	value_function.cc \
	sensor.cc

LIBMLDB_CORE_LINK:= \
	sql_expression rest_entity rest


$(eval $(call library,mldb_core,$(LIBMLDB_CORE_SOURCES),$(LIBMLDB_CORE_LINK)))
