ifneq ($(PREMAKE),1)

#$(shell mkdir -p $(OBJ)/mldb/ext/hoedown/src/)
#$(shell mkdir -p $(OBJ)/mldb/ext/hoedown/bin/)


HOEDOWN_SRC=\
	src/autolink.c \
	src/buffer.c \
	src/document.c \
	src/escape.c \
	src/html.c \
	src/html_blocks.c \
	src/html_smartypants.c \
	src/stack.c \
	src/version.c \

HOEDOWN_WARNING_FLAGS := gcc13+:-Wno-enum-int-mismatch

HOEDOWN_REAL_SRC=$(HOEDOWN_SRC)

#$(eval $(call set_compile_option,bin/hoedown.c bin/smartypants.c,-Imldb/ext/hoedown/src $(HOEDOWN_WARNING_FLAGS)))

$(eval $(call set_compile_option,$(HOEDOWN_SRC),$(HOEDOWN_WARNING_FLAGS)))
$(eval $(call library,hoedown,$(HOEDOWN_REAL_SRC)))
#$(eval $(call program,hoedown,hoedown,bin/hoedown.c))
#$(eval $(call program,smartypants,hoedown,bin/smartypants.c))

# Perfect hashing

$(CWD)/src/html_blocks.c: mldb/ext/hoedown/html_block_names.gperf
	gperf -L ANSI-C -N hoedown_find_block_tag -c -C -E -S 1 --ignore-case -m100 $^ > $@~ && mv $@~ $@

endif
