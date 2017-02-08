# This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

HAS_EXCEPTION_HOOK := 1

# Fortran is required at this point so set it to gfortran (since make sets FC to f77 by default)
FC := gfortran

$(eval $(call include_sub_makes,math utils db stats))
