LIB_CITYHASH_SOURCES := \
	src/city.cc

$(eval $(call library,cityhash,$(LIB_CITYHASH_SOURCES),))
