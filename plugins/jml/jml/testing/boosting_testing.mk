# This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

BOOSTING_TESTING_EXTRA_LIBS:=base utils value_description arch boosting jml_utils

$(eval $(call test,decision_tree_xor_test,boosting utils arch $(BOOSTING_TESTING_EXTRA_LIBS),boost))
$(eval $(call test,split_test,boosting $(BOOSTING_TESTING_EXTRA_LIBS),boost))
$(eval $(call test,decision_tree_multithreaded_test,boosting utils arch $(BOOSTING_TESTING_EXTRA_LIBS),boost))
$(eval $(call test,decision_tree_unlimited_depth_test,boosting utils arch $(BOOSTING_TESTING_EXTRA_LIBS),boost))
$(eval $(call test,glz_classifier_test,boosting utils arch $(BOOSTING_TESTING_EXTRA_LIBS),boost))
$(eval $(call test,probabilizer_test,boosting utils arch $(BOOSTING_TESTING_EXTRA_LIBS),boost))
$(eval $(call test,feature_info_test,boosting utils arch $(BOOSTING_TESTING_EXTRA_LIBS),boost))
$(eval $(call test,weighted_training_test,boosting $(BOOSTING_TESTING_EXTRA_LIBS),boost))
$(eval $(call test,feature_set_test,boosting $(BOOSTING_TESTING_EXTRA_LIBS),boost))

$(eval $(call program,dataset_nan_test,boosting utils arch boosting_tools $(BOOSTING_TESTING_EXTRA_LIBS)))

ifeq ($(CUDA_ENABLED),1)
$(eval $(call test,split_cuda_test,boosting_cuda,boost))
endif # CUDA_ENABLED

$(TESTS)/weighted_training_test: $(BIN)/classifier_training_tool
