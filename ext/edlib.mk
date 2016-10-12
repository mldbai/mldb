EDLIB_CC_FILES:= edlib/src/edlib.cpp
EDLIB_WARNING_OPTIONS:=-Wno-char-subscripts
$(eval $(call set_compile_option,$(EDLIB_CC_FILES),$(EDLIB_WARNING_OPTIONS)))
$(eval $(call library,edlib,$(EDLIB_CC_FILES)))

# Hacky since the test is within a "main", doesn't use boost and is in a cpp
# file rather than a cc.
EDLIB_TEST_FILE:=test/runTests.cpp
$(eval $(call set_compile_option,$(EDLIB_TEST_FILE),-I mldb/ext/edlib/edlib/include))
$(eval $(call program,edlib_test,edlib,$(EDLIB_TEST_FILE)))

ifneq ($(PREMAKE),1)
$(TESTS)/edlib_test.passed: $(BIN)/edlib_test
	@$(BIN)/edlib_test > /dev/null || echo "                 $(COLOR_RED)edlib_test FAILED$(COLOR_RESET)"
	@mkdir -p $(TESTS)
	@touch $(TESTS)/edlib_test.passed
	@echo "                 $(COLOR_GREEN)edlib_test passed$(COLOR_RESET)"

tests: $(BIN)/edlib_test
autotest: $(TESTS)/edlib_test.passed

edlib_test.passed: $(TESTS)/edlib_test.passed
.PHONY: edlib_test.passed
endif
