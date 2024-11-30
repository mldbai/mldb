# 
# When upgrading libgit2 to a new version,
# uncomment these lines and look for missing files between the output and
# the contents of the LIBGIT2_SOURCE variable below.
#
#LIBGIT2_FILES := $(wildcard ext/libgit2/src/*.c ext/libgit2/src/*/*.c)
#LIBGIT2_SOURCE:=$(LIBGIT2_FILES:ext/libgit2/%=%)
#$(warning LIBGIT2_SOURCE=$(LIBGIT2_SOURCE))

LIBGIT2_SOURCE:= \
	src/libgit2/annotated_commit.c \
	src/libgit2/apply.c \
	src/libgit2/attr.c \
	src/libgit2/attrcache.c \
	src/libgit2/attr_file.c \
	src/libgit2/blame.c \
	src/libgit2/blame_git.c \
	src/libgit2/blob.c \
	src/libgit2/branch.c \
	src/libgit2/buffer.c \
	src/libgit2/buf_text.c \
	src/libgit2/cache.c \
	src/libgit2/checkout.c \
	src/libgit2/cherrypick.c \
	src/libgit2/clone.c \
	src/libgit2/commit.c \
	src/libgit2/commit_list.c \
	src/libgit2/config.c \
	src/libgit2/config_cache.c \
	src/libgit2/config_file.c \
	src/libgit2/crlf.c \
	src/libgit2/curl_stream.c \
	src/libgit2/date.c \
	src/libgit2/delta.c \
	src/libgit2/describe.c \
	src/libgit2/diff.c \
	src/libgit2/diff_driver.c \
	src/libgit2/diff_file.c \
	src/libgit2/diff_generate.c \
	src/libgit2/diff_parse.c \
	src/libgit2/diff_print.c \
	src/libgit2/diff_stats.c \
	src/libgit2/diff_tform.c \
	src/libgit2/diff_xdiff.c \
	src/libgit2/errors.c \
	src/libgit2/fetch.c \
	src/libgit2/fetchhead.c \
	src/libgit2/filebuf.c \
	src/libgit2/fileops.c \
	src/libgit2/filter.c \
	src/libgit2/fnmatch.c \
	src/libgit2/global.c \
	src/libgit2/graph.c \
	src/libgit2/hash.c \
	src/libgit2/hashsig.c \
	src/libgit2/ident.c \
	src/libgit2/ignore.c \
	src/libgit2/index.c \
	src/libgit2/indexer.c \
	src/libgit2/iterator.c \
	src/libgit2/merge.c \
	src/libgit2/merge_driver.c \
	src/libgit2/merge_file.c \
	src/libgit2/message.c \
	src/libgit2/mwindow.c \
	src/libgit2/netops.c \
	src/libgit2/notes.c \
	src/libgit2/object_api.c \
	src/libgit2/object.c \
	src/libgit2/odb.c \
	src/libgit2/odb_loose.c \
	src/libgit2/odb_mempack.c \
	src/libgit2/odb_pack.c \
	src/libgit2/oidarray.c \
	src/libgit2/oid.c \
	src/libgit2/openssl_stream.c \
	src/libgit2/pack.c \
	src/libgit2/pack-objects.c \
	src/libgit2/patch.c \
	src/libgit2/patch_generate.c \
	src/libgit2/patch_parse.c \
	src/libgit2/path.c \
	src/libgit2/pathspec.c \
	src/libgit2/pool.c \
	src/libgit2/posix.c \
	src/libgit2/pqueue.c \
	src/libgit2/proxy.c \
	src/libgit2/push.c \
	src/libgit2/rebase.c \
	src/libgit2/refdb.c \
	src/libgit2/refdb_fs.c \
	src/libgit2/reflog.c \
	src/libgit2/refs.c \
	src/libgit2/refspec.c \
	src/libgit2/remote.c \
	src/libgit2/repository.c \
	src/libgit2/reset.c \
	src/libgit2/revert.c \
	src/libgit2/revparse.c \
	src/libgit2/revwalk.c \
	src/libgit2/settings.c \
	src/libgit2/sha1_lookup.c \
	src/libgit2/signature.c \
	src/libgit2/socket_stream.c \
	src/libgit2/sortedcache.c \
	src/libgit2/stash.c \
	src/libgit2/status.c \
	src/libgit2/stransport_stream.c \
	src/libgit2/strmap.c \
	src/libgit2/submodule.c \
	src/libgit2/sysdir.c \
	src/libgit2/tag.c \
	src/libgit2/thread-utils.c \
	src/libgit2/tls_stream.c \
	src/libgit2/trace.c \
	src/libgit2/transaction.c \
	src/libgit2/transport.c \
	src/libgit2/tree.c \
	src/libgit2/tree-cache.c \
	src/libgit2/tsort.c \
	src/libgit2/util.c \
	src/libgit2/varint.c \
	src/libgit2/vector.c \
	src/libgit2/zstream.c \
	src/libgit2/transports/auth.c \
	src/libgit2/transports/auth_negotiate.c \
	src/libgit2/transports/cred.c \
	src/libgit2/transports/cred_helpers.c \
	src/libgit2/transports/git.c \
	src/libgit2/transports/http.c \
	src/libgit2/transports/local.c \
	src/libgit2/transports/smart.c \
	src/libgit2/transports/smart_pkt.c \
	src/libgit2/transports/smart_protocol.c \
	src/libgit2/transports/ssh.c \
	src/libgit2/transports/winhttp.c \
	src/libgit2/unix/map.c \
	src/libgit2/unix/realpath.c \
	src/libgit2/xdiff/xdiffi.c \
	src/libgit2/xdiff/xemit.c \
	src/libgit2/xdiff/xhistogram.c \
	src/libgit2/xdiff/xmerge.c \
	src/libgit2/xdiff/xpatience.c \
	src/libgit2/xdiff/xprepare.c \
	src/libgit2/xdiff/xutils.c \
	deps/http-parser/http_parser.c \

#	src/win32/dir.c \
	src/win32/error.c \
	src/win32/findfile.c \
	src/win32/map.c \
	src/win32/path_w32.c \
	src/win32/posix_w32.c \
	src/win32/precompiled.c \
	src/win32/thread.c \
	src/win32/utf-conv.c \
	src/win32/w32_buffer.c \
	src/win32/w32_crtdbg_stacktrace.c \
	src/win32/w32_stack.c \
	src/win32/w32_util.c \
	src/hash/hash_win32.c \

# 	src/hash/hash_generic.c \

LIBGIT2_LINK_Darwin:=crypto ssl z iconv curl
LIBGIT2_LINK_Linux:=crypto ssl z curl

LIBGIT2_GCC_FLAGS:=
LIBGIT2_CLANG_FLAGS:=-Wno-unknown-warning-option

LIBGIT2_FLAGS:= \
	$(if $(findstring gcc,$(toolchain)),$(LIBGIT2_GCC_FLAGS)) \
	$(if $(findstring clang,$(toolchain)),$(LIBGIT2_CLANG_FLAGS))

# NOTE: to find this, run cmake in the ext/libgit2 directory, and then look at
# CMakeFIles/git2.dir/flags.make to find the ones required.
LIBGIT2_DEFINES_Linux_x86_64:=-D_GNU_SOURCE -DGIT_ARCH_64 -DGIT_CURL -DGIT_OPENSSL -DGIT_SSH -DGIT_THREADS -DGIT_USE_NSEC -DGIT_USE_STAT_MTIM -DHAVE_FUTIMENS -DHAVE_QSORT_R -DOPENSSL_SHA1 -D_FILE_OFFSET_BITS=64 -Dgit2_EXPORTS
LIBGIT2_DEFINES_Linux_arm:=-D_GNU_SOURCE -DGIT_ARCH_32 -DGIT_CURL -DGIT_OPENSSL -DGIT_SSH -DGIT_THREADS -DGIT_USE_NSEC -DGIT_USE_STAT_MTIM -DHAVE_FUTIMENS -DHAVE_QSORT_R -DOPENSSL_SHA1 -D_FILE_OFFSET_BITS=64 -Dgit2_EXPORTS
LIBGIT2_DEFINES_Linux_aarch64:=-D_GNU_SOURCE -DGIT_ARCH_64 -DGIT_CURL -DGIT_OPENSSL -DGIT_SSH -DGIT_THREADS -DGIT_USE_NSEC -DGIT_USE_STAT_MTIM -DHAVE_FUTIMENS -DHAVE_QSORT_R -DOPENSSL_SHA1 -D_FILE_OFFSET_BITS=64 -Dgit2_EXPORTS
LIBGIT2_DEFINES_Darwin_x86_64:=-D_GNU_SOURCE -DGIT_ARCH_64 -DGIT_COMMON_CRYPTO -DGIT_CURL -DGIT_OPENSSL -DGIT_THREADS -DGIT_USE_ICONV -DGIT_USE_NSEC -DGIT_USE_STAT_MTIMESPEC -DHAVE_FUTIMENS -DHAVE_QSORT_R -DHAVE_REGCOMP_L -D_FILE_OFFSET_BITS=64 -Dgit2_EXPORTS # -DGIT_SECURE_TRANSPORT
LIBGIT2_DEFINES_Darwin_arm64:=-D_GNU_SOURCE -DGIT_ARCH_64 -DGIT_COMMON_CRYPTO -DGIT_CURL -DGIT_OPENSSL -DGIT_THREADS -DGIT_USE_ICONV -DGIT_USE_NSEC -DGIT_USE_STAT_MTIMESPEC -DHAVE_FUTIMENS -DHAVE_QSORT_R -DHAVE_REGCOMP_L -D_FILE_OFFSET_BITS=64 -Dgit2_EXPORTS # -DGIT_SECURE_TRANSPORT
LIBGIT2_DEFINES:=$(LIBGIT2_DEFINES_$(OSNAME)_$(ARCH)) $(LIBGIT2_FLAGS)
$(if $(LIBGIT2_DEFINES),,$(error LIBGIT2_DEFINES_$(OSNAME)_$(ARCH) not defined (unknown tuple os $(OSNAME) arch $(ARCH)).  Please define in libgit2.mk))

$(eval $(call set_compile_option,$(LIBGIT2_SOURCE),-Imldb/ext/libgit2/include -Imldb/ext/libgit2/include/git2 -Imldb/ext/libgit2/src/util -Imldb/ext/libgit2/deps/http-parser $(OPENSSL_INCLUDE_FLAGS) $(LIBGIT2_DEFINES) -Wno-maybe-uninitialized -Wno-bool-operation -Wno-unused-function -Wno-stringop-truncation  -Wno-deprecated-declarations))

$(eval $(call library,git2,$(LIBGIT2_SOURCE),$(LIBGIT2_LINK_$(OSNAME))))
