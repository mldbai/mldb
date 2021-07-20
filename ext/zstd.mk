ZSTD_SOURCE := \
	lib/compress/zstd_compress_superblock.c \
	lib/compress/zstdmt_compress.c \
	lib/compress/zstd_double_fast.c \
	lib/compress/zstd_fast.c \
	lib/compress/zstd_compress_sequences.c \
	lib/compress/zstd_ldm.c \
	lib/compress/hist.c \
	lib/compress/zstd_compress.c \
	lib/compress/zstd_lazy.c \
	lib/compress/zstd_compress_literals.c \
	lib/compress/huf_compress.c \
	lib/compress/zstd_opt.c \
	lib/compress/fse_compress.c \
	lib/dictBuilder/cover.c \
	lib/dictBuilder/divsufsort.c \
	lib/dictBuilder/fastcover.c \
	lib/dictBuilder/zdict.c \
	lib/decompress/zstd_ddict.c \
	lib/decompress/huf_decompress.c \
	lib/decompress/zstd_decompress.c \
	lib/decompress/zstd_decompress_block.c \
	lib/legacy/zstd_v05.c \
	lib/legacy/zstd_v01.c \
	lib/legacy/zstd_v06.c \
	lib/legacy/zstd_v02.c \
	lib/legacy/zstd_v07.c \
	lib/legacy/zstd_v03.c \
	lib/legacy/zstd_v04.c \
	lib/common/entropy_common.c \
	lib/common/fse_decompress.c \
	lib/common/debug.c \
	lib/common/xxhash.c \
	lib/common/pool.c \
	lib/common/threading.c \
	lib/common/zstd_common.c \
	lib/common/error_private.c \
	lib/deprecated/zbuff_common.c \
	lib/deprecated/zbuff_decompress.c \
	lib/deprecated/zbuff_compress.c \


ZSTD_BINARY_SOURCE:= \
	programs/zstdcli.c \
	programs/fileio.c \
	programs/datagen.c \
	programs/dibio.c \
	programs/util.c \
	programs/zstdcli_trace.c \
	programs/benchzstd.c \
	programs/benchfn.c \
	programs/timefn.c \

ZSTD_GCC_FLAGS:=-Wno-deprecated-declarations -Wno-maybe-uninitialized
ZSTD_CLANG_FLAGS:=-Wno-deprecated-declarations -Wno-maybe-uninitialized -Wno-unknown-warning-option

ZSTD_FLAGS:= \
	$(if $(findstring gcc,$(toolchain)),$(ZSTD_GCC_FLAGS)) \
	$(if $(findstring clang,$(toolchain)),$(ZSTD_CLANG_FLAGS))

$(eval $(call set_compile_option,$(ZSTD_SOURCE) $(ZSTD_BINARY_SOURCE),-Imldb/ext/zstd/include -I$(CWD)/lib -I$(CWD)/lib/common -I$(CWD)/lib/dictBuilder -I$(CWD)/lib/legacy -DZSTD_LEGACY_SUPPORT=1 $(ZSTD_FLAGS)))

$(eval $(call library,zstd,$(ZSTD_SOURCE)))
$(eval $(call program,zstd,zstd,$(ZSTD_BINARY_SOURCE)))
