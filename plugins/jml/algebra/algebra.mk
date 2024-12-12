# This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

LIBALGEBRA_SOURCES := \
        least_squares.cc \
        irls.cc \
        lapack.cc \
	ilaenv.c \
        svd.cc \
        matrix.cc \
        matrix_ops.cc \

$(eval $(call add_sources,$(LIBALGEBRA_SOURCES)))

LIBALGEBRA_LINK :=	utils lapack blas db arch base value_description boost_timer

$(eval $(call library,algebra,$(LIBALGEBRA_SOURCES),$(LIBALGEBRA_LINK)))

$(eval $(call include_sub_make,algebra_testing,testing))
