CLANG_VERSION:=$(shell clang++ --version | head -n1 | awk '{ print $4; }' | sed 's/-*//g')
CXX_VERSION?=$(CLANG_VERSION)
CXX := $(COMPILER_CACHE) clang++

$(warning building with clang++ version $(CLANG_VERSION))

CLANGXXWARNINGFLAGS?=-Wall -Werror -Wno-sign-compare -Woverloaded-virtual -Wno-deprecated-declarations -Wno-deprecated -Winit-self -Qunused-arguments -Wno-mismatched-tags

CXXFLAGS ?= $(ARCHFLAGS) $(INCLUDE) $(CLANGXXWARNINGFLAGS) -pipe -ggdb $(foreach dir,$(LOCAL_INCLUDE_DIR),-I$(dir)) -std=c++0x 
CXXNODEBUGFLAGS := -O3 -DBOOST_DISABLE_ASSERTS -DNDEBUG 
CXXDEBUGFLAGS := -O0 -g3

CXXLINKFLAGS = -rdynamic $(foreach DIR,$(PWD)/$(BIN) $(PWD)/$(LIB),-L$(DIR) -Wl,--rpath-link,$(DIR)) -Wl,--rpath,\$$ORIGIN/../bin -Wl,--rpath,\$$ORIGIN/../lib -Wl,--copy-dt-needed-entries -Wl,--no-as-needed
CXXLIBRARYFLAGS = -shared $(CXXLINKFLAGS) -lpthread
CXXEXEFLAGS =$(CXXLINKFLAGS) -lpthread
CXXEXEPOSTFLAGS = $(if $(MEMORY_ALLOC_LIBRARY),-l$(MEMORY_ALLOC_LIBRARY))



CC ?= $(COMPILER_CACHE) clang
GCCWARNINGFLAGS?=-Wall -Werror -Wno-sign-compare -fno-strict-overflow -Wno-unused-but-set-variable
CFLAGS ?= $(ARCHFLAGS) $(INCLUDE) $(GCCWARNINGFLAGS) -pipe -O3 -g -DNDEBUG -fno-omit-frame-pointer
CDEBUGFLAGS := -O0 -g
CNODEBUGFLAGS := -O3 -g -DNDEBUG 

FC ?= gfortran
FFLAGS ?= $(ARCHFLAGS) -I. -fPIC
