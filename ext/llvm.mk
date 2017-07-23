ifneq ($(PREMAKE),1)

LLVM_BUILD_CPU:=X86
#LLVM_BUILD_CPU:=X86;AArch64;ARM;PowerPC;RISCV 

LLVM_BUILD_GPU:=NVPTX
#LLVM_BUILD_GPU:=NVPTX;AMDGPU

LLVM_BUILD_TARGETS:=$(LLVM_BUILD_CPU);$(LLVM_BUILD_GPU)

$(warning LLVM_BUILD_TARGETS=$(LLVM_BUILD_TARGETS))

$(TMP)/llvm/Makefile:	$(DEPENDS_ON_CMAKE)
	@echo " $(COLOR_BLUE)[CONFIG EXTERN]$(COLOR_RESET)                      	llvm"
	@mkdir -p $(TMP)/llvm
	@cd $(TMP)/llvm && $(CMAKE) -G "Unix Makefiles" -DCMAKE_INSTALL_PREFIX=$(PWD)/$(BUILD)/$(ARCH) -DLLVM_BUILD_LLVM_DYLIB=ON -DLLVM_BUILD_TOOLS=OFF -DLLVM_CCACHE_BUILD=ON -DCMAKE_BUILD_TYPE=Release -DLLVM_TARGETS_TO_BUILD="$(LLVM_BUILD_TARGETS)" $(PWD)/mldb/ext/llvm > $(TMP)/llvm-bootstrap.log 2>&1 || (cat $(TMP)/llvm-bootstrap.log && echo "$(COLOR_RED)Error bootstrapping llvm$(COLOR_RESET)" && false)

$(LIB)/libLLVM.so:	$(TMP)/llvm/Makefile
	@echo "   $(COLOR_BLUE)[MAKE EXTERN]$(COLOR_RESET)                      	llvm"
	@+make -j -C $(TMP)/llvm install > $(TMP)/llvm-make.log 2>&1 || (cat $(TMP)/llvm-make.log && echo "$(COLOR_RED)Error building llvm$(COLOR_RESET)" && false)
	@echo "   $(COLOR_BLUE)[DONE EXTERN]$(COLOR_RESET)                      	llvm"

endif
