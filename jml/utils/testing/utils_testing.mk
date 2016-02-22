# This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

$(eval $(call test,parse_context_test,utils arch,boost))
$(eval $(call test,configuration_test,utils arch,boost))
$(eval $(call test,environment_test,utils arch,boost))
$(eval $(call test,compact_vector_test,arch,boost))
$(eval $(call test,circular_buffer_test,arch,boost))
$(eval $(call test,lightweight_hash_test,arch utils,boost))
$(eval $(call test,string_functions_test,arch utils,boost))

$(eval $(call test,csv_parsing_test,arch utils,boost))
