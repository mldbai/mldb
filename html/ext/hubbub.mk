ifneq ($(PREMAKE),1)

$(shell mkdir -p $(OBJ)/mldb/ext/libhubbub/src/treebuilder $(OBJ)/mldb/ext/libhubbub/src/tokeniser $(OBJ)/mldb/ext/libhubbub/src/utils $(OBJ)/mldb/ext/libhubbub/src/charset)
$(shell mkdir -p $(OBJ)/mldb/ext/libhubbub/bin/)

LIBHUBBUB_SRC=\
	src/treebuilder/before_head.c \
	src/treebuilder/after_after_body.c \
	src/treebuilder/in_table.c \
	src/treebuilder/generic_rcdata.c \
	src/treebuilder/treebuilder.c \
	src/treebuilder/in_caption.c \
	src/treebuilder/after_frameset.c \
	src/treebuilder/in_foreign_content.c \
	src/treebuilder/in_body.c \
	src/treebuilder/in_head_noscript.c \
	src/treebuilder/in_cell.c \
	src/treebuilder/in_column_group.c \
	src/treebuilder/in_select_in_table.c \
	src/treebuilder/in_head.c \
	src/treebuilder/initial.c \
	src/treebuilder/after_body.c \
	src/treebuilder/after_head.c \
	src/treebuilder/in_select.c \
	src/treebuilder/in_frameset.c \
	src/treebuilder/in_table_body.c \
	src/treebuilder/in_row.c \
	src/treebuilder/after_after_frameset.c \
	src/treebuilder/before_html.c \
	src/parser.c \
	src/charset/detect.c \
	src/tokeniser/entities.c \
	src/tokeniser/tokeniser.c \
	src/utils/string.c \
	src/utils/errors.c \

$(eval $(call set_compile_option,$(LIBHUBBUB_SRC),-Imldb/html/ext -Imldb/html/ext/libhubbub/src -I$(TMP)))

$(CWD)/src/tokeniser/entities.c: $(CWD)/src/tokeniser/entities.inc

HUBBUB_CWD:=$(CWD)

$(CWD)/src/tokeniser/entities.inc:
	cd $(HUBBUB_CWD) && perl build/make-entities.pl

$(eval $(call library,hubbub,$(LIBHUBBUB_SRC),parserutils))


endif
