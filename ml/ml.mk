# This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

# ml.mk

$(eval $(call include_sub_makes,algebra jml neural tsne))

LIBML_SOURCES := \
	dense_classifier.cc \
	separation_stats.cc \
	dense_classifier_scorer.cc \
	dense_feature_generator.cc \
	data_partition.cc \
	scorer.cc \
	prediction_accumulator.cc \
	bucketing_probabilizer.cc \
	distribution_pooler.cc \
	kmeans.cc \
	em.cc \
	value_descriptions.cc \
	confidence_intervals.cc \
	svd_utils.cc \
    randomforest.cc


LIBML_LINK := boosting neural boost_filesystem jsoncpp types value_description algebra

$(eval $(call library,ml,$(LIBML_SOURCES),$(LIBML_LINK)))

$(eval $(call include_sub_make,ml_testing,testing,ml_testing.mk))
