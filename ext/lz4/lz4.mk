LIB_LZ4_SOURCES := \
	lz4.c \
	lz4hc.c \
	lz4frame.c

$(eval $(call library,lz4,$(LIB_LZ4_SOURCES),xxhash))

# NOTE: the source to this program is under the GPL, and so we can ONLY
# build a program; we can't copy the code or make it a library

LZ4_GPL_COMMAND_LINE_SOURCES:=lz4cli.c bench.c datagen.c lz4io.c

$(eval $(call set_compile_option,$(LIB_LZ4_SOURCES) $(LZ4_GPL_COMMAND_LINE_SOURCES),-Imldb/ext/xxhash))

$(eval $(call program,lz4cli,lz4,$(LZ4_GPL_COMMAND_LINE_SOURCES)))
