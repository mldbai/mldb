ifneq ($(PREMAKE),1)

$(HOSTBIN)/cmake: $(TMP)/$(HOSTARCH)/cmake/Makefile
	@echo "   $(COLOR_BLUE)[MAKE EXTERN]$(COLOR_RESET)                      	cmake"
	@+make -C $(TMP)/$(HOSTARCH)/cmake install > $(TMP)/$(HOSTARCH)/cmake-make.log 2>&1 || (cat $(TMP)/$(HOSTARCH)/cmake-make.log && echo "$(COLOR_RED)Error building cmake$(COLOR_RESET)" && false)
	@echo "   $(COLOR_BLUE)[DONE EXTERN]$(COLOR_RESET)                      	cmake"

$(TMP)/$(HOSTARCH)/cmake/Makefile: $(TMP)/$(HOSTARCH)/cmake/bootstrap
	@echo " $(COLOR_BLUE)[CONFIG EXTERN]$(COLOR_RESET)                      	cmake"
	@cd $(TMP)/$(HOSTARCH)/cmake && env CXX="$(lastword $(CXX))" CC="$(lastword $(CC))" ./bootstrap --system-curl --system-zlib --system-bzip2 --system-liblzma --system-libarchive --prefix=$(PWD)/$(BUILD)/$(HOSTARCH) --parallel=16 $(if $(findstring ccache,$(CXX)),--enable-ccache) > $(TMP)/$(HOSTARCH)/cmake-bootstrap.log 2>&1 || (cat $(TMP)/$(HOSTARCH)/cmake-bootstrap.log && echo "$(COLOR_RED)Error bootstrapping cmake$(COLOR_RESET)" && false)

$(TMP)/$(HOSTARCH)/cmake/bootstrap: $(TMP)/$(HOSTARCH)/.dir_exists
	@echo "   $(COLOR_BLUE)[COPY EXTERN]$(COLOR_RESET)                      	cmake"
	@rm -rf $(TMP)/$(HOSTARCH)/cmake $(TMP)/$(HOSTARCH)/cmake-tmp
	@cp -r mldb/ext/cmake $(TMP)/$(HOSTARCH)/cmake-tmp
	@mv $(TMP)/$(HOSTARCH)/cmake-tmp $(TMP)/$(HOSTARCH)/cmake

CMAKE := $(HOSTBIN)/cmake

endif
