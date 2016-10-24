GIFLIB_SOURCE := $(addprefix lib/,dgif_lib.c  gifalloc.c  gif_font.c  openbsd-reallocarray.c egif_lib.c  gif_err.c   gif_hash.c  quantize.c)

$(eval $(call library,giflib,$(GIFLIB_SOURCE)))
