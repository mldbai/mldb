ifneq ($(PREMAKE),1)

$(shell mkdir -p $(OBJ)/mldb/ext/libparserutils/src/charset/codecs $(OBJ)/mldb/ext/libparserutils/src/charset/encodings $(OBJ)/mldb/ext/libparserutils/src/utils $(OBJ)/mldb/ext/libparserutils/src/input)
$(shell mkdir -p $(OBJ)/mldb/ext/libparserutils/bin/)


LIBPARSERUTILS_SRC=\
	src/charset/codecs/codec_utf16.c \
	src/charset/codecs/codec_ext8.c \
	src/charset/codecs/codec_ascii.c \
	src/charset/codecs/codec_8859.c \
	src/charset/codecs/codec_utf8.c \
	src/charset/codec.c \
	src/charset/encodings/utf8.c \
	src/charset/encodings/utf16.c \
	src/charset/aliases.c \
	src/input/filter.c \
	src/input/inputstream.c \
	src/utils/errors.c \
	src/utils/vector.c \
	src/utils/stack.c \
	src/utils/buffer.c

$(eval $(call set_compile_option,$(LIBPARSERUTILS_SRC),-Imldb/html/ext -Imldb/html/ext/libparserutils/src))

$(eval $(call library,parserutils,$(LIBPARSERUTILS_SRC)))


endif
