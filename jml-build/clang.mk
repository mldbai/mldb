# Note: clang 3.6 won't compile MLDB if GCC6 is installed

find_command=$(foreach command,$(firstword $(1)),$(if $(shell which $(command)),$(command),$(call find_command,$(wordlist 2,10000,$(1)))))

CLANGXX?=$(call find_command,clang++ clang++-13 clang++-12 clang++-11 clang++-10 clang++-9 clang++-8)
CLANG?=$(call find_command,clang clang-13 clang-12 clang-11 clang-10 clang-9 clang-8)

CLANG_VERSION:=$(shell $(CLANGXX) --version | head -n1 | awk '{ print $$4; }' | sed 's/-*//g')
CXX_VERSION?=$(CLANG_VERSION)
CXX := $(COMPILER_CACHE) $(CLANGXX)
BUILDING_WITH_CLANG:=1

$(warning building with clang++ version $(CLANG_VERSION))

CLANGXXWARNINGFLAGS?=-Wall -Werror -Wno-sign-compare -Woverloaded-virtual -Wno-deprecated-declarations -Wno-deprecated -Winit-self -Qunused-arguments -Wno-mismatched-tags -Wno-unused-function -ftemplate-backtrace-limit=0 -Wno-inconsistent-missing-override

CLANG_SANITIZER_address_FLAGS:=-fsanitize=address
CLANG_SANITIZER_memory_FLAGS:=-fsanitize=memory
CLANG_SANITIZER_thread_FLAGS:=-fsanitize=thread
CLANG_SANITIZER_undefined_FLAGS:=-fsanitize=undefined -fno-sanitize-recover=undefined -fno-sanitize=vptr -fvisibility=default

CXXFLAGS ?= $(ARCHFLAGS) $(INCLUDE) $(CLANGXXWARNINGFLAGS) -pipe -ggdb $(foreach dir,$(LOCAL_INCLUDE_DIR),-I$(dir)) -std=c++17 $(foreach sanitizer,$(sanitizers),$(CLANG_SANITIZER_$(sanitizer)_FLAGS) )
CXXNODEBUGFLAGS := -O3 -DBOOST_DISABLE_ASSERTS -DNDEBUG 
CXXDEBUGFLAGS := -O0 -g3

CXXLINKFLAGS_Darwin = -rdynamic $(foreach DIR,$(PWD)/$(BIN) $(PWD)/$(LIB),-L $(DIR))
CXXLINKFLAGS_Linux = -rdynamic $(foreach DIR,$(PWD)/$(BIN) $(PWD)/$(LIB),-L$(DIR) -Wl,--rpath-link,$(DIR)) -Wl,--rpath,\$$ORIGIN/../bin -Wl,--rpath,\$$ORIGIN/../lib -Wl,--copy-dt-needed-entries -Wl,--no-as-needed
CXXLINKFLAGS = $(CXXLINKFLAGS_$(OSNAME))
CXXLIBRARYFLAGS = -shared $(CXXLINKFLAGS) -lpthread
CXXEXEFLAGS =$(CXXLINKFLAGS) -lpthread
CXXEXEPOSTFLAGS = $(if $(MEMORY_ALLOC_LIBRARY),-l$(MEMORY_ALLOC_LIBRARY))



CC := $(COMPILER_CACHE) $(CLANG)
GCCWARNINGFLAGS?=-Wall -Werror -Wno-sign-compare -fno-strict-overflow
CFLAGS ?= $(ARCHFLAGS) $(INCLUDE) $(GCCWARNINGFLAGS) -pipe -O1 -g -DNDEBUG -fno-omit-frame-pointer
CDEBUGFLAGS := -O0 -g
CNODEBUGFLAGS := -O2 -g -DNDEBUG 

FC ?= gfortran
FFLAGS ?= $(ARCHFLAGS) -I. -fPIC

# Library name of filesystem
#STD_FILESYSTEM_LIBNAME:=stdc++fs
