# This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

TYPES_TESTING_EXTRA_LIBS:=types arch value_description base

$(eval $(call test,date_test,$(TYPES_TESTING_EXTRA_LIBS),boost))
$(eval $(call test,localdate_test,$(TYPES_TESTING_EXTRA_LIBS),boost valgrind))
$(eval $(call test,string_test,$(TYPES_TESTING_EXTRA_LIBS) boost_regex icui18n icuuc,boost))
$(eval $(call set_compile_option,string_test.cc,-I$(ICU_INCLUDE_PATH) -Wno-unused))
$(eval $(call test,json_handling_test,$(TYPES_TESTING_EXTRA_LIBS),boost))
$(eval $(call test,value_description_test,$(TYPES_TESTING_EXTRA_LIBS),boost))
$(eval $(call test,periodic_utils_test,$(TYPES_TESTING_EXTRA_LIBS),boost))
$(eval $(call test,reader_test,$(TYPES_TESTING_EXTRA_LIBS),boost))
$(eval $(call test,json_parsing_test,$(TYPES_TESTING_EXTRA_LIBS),boost))
$(eval $(call test,any_test,any $(TYPES_TESTING_EXTRA_LIBS),boost))
$(eval $(call test,decode_uri_test,$(TYPES_TESTING_EXTRA_LIBS),boost))

