# Makefile for the fasttext library

FASTTEXT_SOURCE := \
    src/args.cc \
    src/dictionary.cc \
    src/matrix.cc \
    src/vector.cc \
    src/model.cc \
    src/utils.cc \
    src/fasttext.cc \

$(eval $(call set_compile_option,$(FASTTEXT_SOURCE),-Imldb/ext/fasttext/src))

$(eval $(call library,fasttext,$(FASTTEXT_SOURCE)))

