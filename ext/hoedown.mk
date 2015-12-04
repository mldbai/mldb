ifneq ($(PREMAKE),1)

$(shell mkdir -p $(OBJ)/mldb/ext/hoedown/src/)
$(shell mkdir -p $(OBJ)/mldb/ext/hoedown/bin/)


HOEDOWN_SRC=\
	src/autolink.o \
	src/buffer.o \
	src/document.o \
	src/escape.o \
	src/html.o \
	src/html_blocks.o \
	src/html_smartypants.o \
	src/stack.o \
	src/version.o

HOEDOWN_REAL_SRC=$(HOEDOWN_SRC:%.o=%.c)

$(eval $(call set_compile_option,bin/hoedown.c bin/smartypants.c,-Imldb/ext/hoedown/src))

$(eval $(call library,hoedown,$(HOEDOWN_REAL_SRC)))
$(eval $(call program,hoedown,hoedown,bin/hoedown.c))
$(eval $(call program,smartypants,hoedown,bin/smartypants.c))

# Perfect hashing

$(CWD)/src/html_blocks.c: mldb/ext/hoedown/html_block_names.gperf
	gperf -L ANSI-C -N hoedown_find_block_tag -c -C -E -S 1 --ignore-case -m100 $^ > $@~ && mv $@~ $@

endif
