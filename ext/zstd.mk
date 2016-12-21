ZSTD_SOURCE := \
	lib/decompress/zstd_decompress.c \
	lib/decompress/huf_decompress.c \
	lib/common/entropy_common.c \
	lib/common/error_private.c \
	lib/common/fse_decompress.c \
	lib/common/xxhash.c \
	lib/common/zstd_common.c \
	lib/compress/zstd_compress.c \
	lib/compress/fse_compress.c \
	lib/compress/huf_compress.c \
	lib/legacy/zstd_v01.c \
	lib/legacy/zstd_v02.c \
	lib/legacy/zstd_v03.c \
	lib/legacy/zstd_v04.c \
	lib/legacy/zstd_v05.c \
	lib/legacy/zstd_v06.c \
	lib/legacy/zstd_v07.c \
	lib/dictBuilder/divsufsort.c \
	lib/dictBuilder/zdict.c \

ZSTD_BINARY_SOURCE:= \
	programs/zstdcli.c \
	programs/fileio.c \
	programs/bench.c \
	programs/datagen.c \
	programs/dibio.c

$(eval $(call set_compile_option,$(ZSTD_SOURCE) $(ZSTD_BINARY_SOURCE),-Imldb/ext/zstd/include -I$(CWD)/lib -I$(CWD)/lib/common -I$(CWD)/lib/dictBuilder -I$(CWD)/lib/legacy -DZSTD_LEGACY_SUPPORT=1))

$(eval $(call library,zstd,$(ZSTD_SOURCE)))
$(eval $(call program,zstd,zstd,$(ZSTD_BINARY_SOURCE)))
