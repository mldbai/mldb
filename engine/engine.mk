LIBMLDB_ENGINE_SOURCES:= \
	dataset_utils.cc \
	analytics.cc \
	dataset_scope.cc \
	bound_queries.cc \
	forwarded_dataset.cc \
	column_scope.cc \
	bucket.cc \

LIBMLDB_ENGINE_LINK:= \
	sql_expression credentials mldb_core


$(eval $(call library,mldb_engine,$(LIBMLDB_ENGINE_SOURCES),$(LIBMLDB_ENGINE_LINK)))
