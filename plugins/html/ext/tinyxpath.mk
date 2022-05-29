ifneq ($(PREMAKE),1)

LIBTINYXPATH_SRC=\
	htmlutil.cpp main.cpp \
	tinystr.cpp tinyxml.cpp tinyxmlerror.cpp tinyxmlparser.cpp \
	action_store.cpp lex_util.cpp node_set.cpp tokenlist.cpp xml_util.cpp \
	xpath_expression.cpp xpath_processor.cpp xpath_stack.cpp xpath_stream.cpp \
	xpath_syntax.cpp xpath_static.cpp

$(eval $(call set_compile_option,$(LIBTINYXPATH_SRC),-Imldb/html/ext -Imldb/html/ext/libtinyxpath/src -I$(TMP) -Wno-switch -Wno-parentheses -Wno-unused-variable $(if $(findstring gcc,$(toolchain)),-Wno-maybe-uninitialized) $(if $(findstring clang,$(toolchain)),-Wno-sometimes-uninitialized -Wno-unused-but-set-variable)))

TINYXPATH_CWD:=$(CWD)

$(eval $(call mldb_plugin_library,html,tinyxpath,$(LIBTINYXPATH_SRC),tinyxml2))

endif
