# 
# When upgrading libgit2 to a new version,
# uncomment these lines and look for missing files between the output and
# the contents of the LIBGIT2_SOURCE variable below.
#
#LIBGIT2_FILES := $(wildcard ext/libgit2/src/*.c ext/libgit2/src/*/*.c)
#LIBGIT2_SOURCE:=$(LIBGIT2_FILES:ext/libgit2/%=%)
#$(warning LIBGIT2_SOURCE=$(LIBGIT2_SOURCE))

LIBGIT2_SOURCE:= \
	src/annotated_commit.c \
	src/apply.c \
	src/attr.c \
	src/attrcache.c \
	src/attr_file.c \
	src/blame.c \
	src/blame_git.c \
	src/blob.c \
	src/branch.c \
	src/buffer.c \
	src/buf_text.c \
	src/cache.c \
	src/checkout.c \
	src/cherrypick.c \
	src/clone.c \
	src/commit.c \
	src/commit_list.c \
	src/config.c \
	src/config_cache.c \
	src/config_file.c \
	src/crlf.c \
	src/curl_stream.c \
	src/date.c \
	src/delta.c \
	src/describe.c \
	src/diff.c \
	src/diff_driver.c \
	src/diff_file.c \
	src/diff_generate.c \
	src/diff_parse.c \
	src/diff_print.c \
	src/diff_stats.c \
	src/diff_tform.c \
	src/diff_xdiff.c \
	src/errors.c \
	src/fetch.c \
	src/fetchhead.c \
	src/filebuf.c \
	src/fileops.c \
	src/filter.c \
	src/fnmatch.c \
	src/global.c \
	src/graph.c \
	src/hash.c \
	src/hashsig.c \
	src/ident.c \
	src/ignore.c \
	src/index.c \
	src/indexer.c \
	src/iterator.c \
	src/merge.c \
	src/merge_driver.c \
	src/merge_file.c \
	src/message.c \
	src/mwindow.c \
	src/netops.c \
	src/notes.c \
	src/object_api.c \
	src/object.c \
	src/odb.c \
	src/odb_loose.c \
	src/odb_mempack.c \
	src/odb_pack.c \
	src/oidarray.c \
	src/oid.c \
	src/openssl_stream.c \
	src/pack.c \
	src/pack-objects.c \
	src/patch.c \
	src/patch_generate.c \
	src/patch_parse.c \
	src/path.c \
	src/pathspec.c \
	src/pool.c \
	src/posix.c \
	src/pqueue.c \
	src/proxy.c \
	src/push.c \
	src/rebase.c \
	src/refdb.c \
	src/refdb_fs.c \
	src/reflog.c \
	src/refs.c \
	src/refspec.c \
	src/remote.c \
	src/repository.c \
	src/reset.c \
	src/revert.c \
	src/revparse.c \
	src/revwalk.c \
	src/settings.c \
	src/sha1_lookup.c \
	src/signature.c \
	src/socket_stream.c \
	src/sortedcache.c \
	src/stash.c \
	src/status.c \
	src/stransport_stream.c \
	src/strmap.c \
	src/submodule.c \
	src/sysdir.c \
	src/tag.c \
	src/thread-utils.c \
	src/tls_stream.c \
	src/trace.c \
	src/transaction.c \
	src/transport.c \
	src/tree.c \
	src/tree-cache.c \
	src/tsort.c \
	src/util.c \
	src/varint.c \
	src/vector.c \
	src/zstream.c \
	src/transports/auth.c \
	src/transports/auth_negotiate.c \
	src/transports/cred.c \
	src/transports/cred_helpers.c \
	src/transports/git.c \
	src/transports/http.c \
	src/transports/local.c \
	src/transports/smart.c \
	src/transports/smart_pkt.c \
	src/transports/smart_protocol.c \
	src/transports/ssh.c \
	src/transports/winhttp.c \
	src/unix/map.c \
	src/unix/realpath.c \
	src/xdiff/xdiffi.c \
	src/xdiff/xemit.c \
	src/xdiff/xhistogram.c \
	src/xdiff/xmerge.c \
	src/xdiff/xpatience.c \
	src/xdiff/xprepare.c \
	src/xdiff/xutils.c \
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


# NOTE: to find this, run cmake in the ext/libgit2 directory, and then look at
# CMakeFIles/git2.dir/flags.make to find the ones required.
LIBGIT2_DEFINES_x86_64:=-D_GNU_SOURCE -DGIT_ARCH_64 -DGIT_CURL -DGIT_OPENSSL -DGIT_SSH -DGIT_THREADS -DGIT_USE_NSEC -DGIT_USE_STAT_MTIM -DHAVE_FUTIMENS -DHAVE_QSORT_R -DOPENSSL_SHA1 -D_FILE_OFFSET_BITS=64 -Dgit2_EXPORTS
LIBGIT2_DEFINES_arm:=-D_GNU_SOURCE -DGIT_ARCH_32 -DGIT_CURL -DGIT_OPENSSL -DGIT_SSH -DGIT_THREADS -DGIT_USE_NSEC -DGIT_USE_STAT_MTIM -DHAVE_FUTIMENS -DHAVE_QSORT_R -DOPENSSL_SHA1 -D_FILE_OFFSET_BITS=64 -Dgit2_EXPORTS
LIBGIT2_DEFINES_aarch64:=-D_GNU_SOURCE -DGIT_ARCH_64 -DGIT_CURL -DGIT_OPENSSL -DGIT_SSH -DGIT_THREADS -DGIT_USE_NSEC -DGIT_USE_STAT_MTIM -DHAVE_FUTIMENS -DHAVE_QSORT_R -DOPENSSL_SHA1 -D_FILE_OFFSET_BITS=64 -Dgit2_EXPORTS
LIBGIT2_DEFINES:=$(LIBGIT2_DEFINES_$(ARCH))
$(if $(LIBGIT2_DEFINES),,$(error LIBGIT2_DEFINES_$(ARCH) not defined (unknown arch $(ARCH)).  Please define in libgit2.mk))

$(eval $(call set_compile_option,$(LIBGIT2_SOURCE),-Imldb/ext/libgit2/include -Imldb/ext/libgit2/src -Imldb/ext/libgit2/deps/http-parser $(LIBGIT2_DEFINES)))

$(eval $(call library,git2,$(LIBGIT2_SOURCE)))
