LIB_LZ4_SOURCES := \
	lz4.c \
	lz4hc.c

$(eval $(call library,lz4,$(LIB_LZ4_SOURCES),))
