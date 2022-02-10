# This file is part of MLDB. Copyright 2022 Jeremy Barnes. All rights reserved.

$(warning CWD=$(CWD))

$(CWD)/ukl_auto_macros.h:	$(CWD)/ukl_auto_macro_gen.py
	$< > $@~ && mv $@~ $@

