# This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

$(eval $(call test,date_test,types arch,boost))
$(eval $(call test,localdate_test,types arch,boost valgrind))
$(eval $(call test,id_test,types,boost valgrind))
$(eval $(call test,string_test,types arch boost_regex,boost))
$(eval $(call test,json_handling_test,types arch value_description,boost))
$(eval $(call test,value_description_test,types arch value_description,boost))
$(eval $(call test,value_instance_test,types arch value_description,boost))
$(eval $(call test,periodic_utils_test,types,boost))
$(eval $(call program,id_profile,types))
$(eval $(call test,reader_test,jsoncpp arch types,boost))
$(eval $(call test,json_parsing_test,types arch,boost))
$(eval $(call test,any_test,any types arch,boost))
