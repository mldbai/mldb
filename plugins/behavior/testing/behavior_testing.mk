# This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

# mldb.ai behavior testing code


#$(eval $(call set_compile_option,$(notdir $(wildcard pro/behavior/testing/*.cc)),-Ipro))

BEHAVIOR_TESTING_EXTRA_LINK:=arch value_description types base vfs db gc

$(eval $(call test,behavior_domain_test,behavior test_utils $(BEHAVIOR_TESTING_EXTRA_LINK),boost timed))
$(eval $(call test,mutable_behavior_domain_test,behavior test_utils $(BEHAVIOR_TESTING_EXTRA_LINK),boost timed))
$(eval $(call test,mapped_behavior_domain_test,behavior test_utils $(BEHAVIOR_TESTING_EXTRA_LINK),boost timed))
$(eval $(call test,behavior_domain_valgrind_test,behavior $(BEHAVIOR_TESTING_EXTRA_LINK),boost valgrind manual))
$(eval $(call test,boolean_expression_test,behavior $(BEHAVIOR_TESTING_EXTRA_LINK),boost timed))
#$(eval $(call test,bridged_behavior_domain_test,behavior,boost timed)) # about to be removed
$(eval $(call test,behavior_manager_test,behavior test_utils $(BEHAVIOR_TESTING_EXTRA_LINK),boost))
$(eval $(call test,id_serialization_test,behavior test_utils $(BEHAVIOR_TESTING_EXTRA_LINK),boost))
$(eval $(call test,root_behavior_manager_test,behavior test_utils $(BEHAVIOR_TESTING_EXTRA_LINK),boost manual))
$(eval $(call test,tranches_test,behavior ,boost))

#$(eval $(call test,guarded_fs_lock_test,behavior test_utils arch,boost))

$(eval $(call test,mutable_behavior_stress_test,behavior $(BEHAVIOR_TESTING_EXTRA_LINK),boost manual))
$(eval $(call program,mutable_behavior_bench,base arch types behavior test_utils utils boost_program_options))
$(eval $(call program,behavior_domain_read_bench,base arch types boost_program_options behavior test_utils))

$(eval $(call test,id_test,behavior $(BEHAVIOR_TESTING_EXTRA_LINK),boost))
$(eval $(call program,id_profile,types behavior))
$(eval $(call test,legacy_behavior_file_test,behavior test_utils $(BEHAVIOR_TESTING_EXTRA_LINK),boost))
