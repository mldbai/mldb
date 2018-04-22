GCC?=$(if $(DEFAULTGCC),$(DEFAULTGCC),gcc)
GXX?=$(if $(DEFAULTGXX),$(DEFAULTGXX),g++)
GCC_VERSION:=$(shell $(GXX) --version | head -n1 | sed 's/.* //g')
CXX_VERSION?=$(GCC_VERSION)
CXX:=$(COMPILER_CACHE) $(GXX)
CC:=$(COMPILER_CACHE) $(GCC)

GXXWARNINGFLAGS?=-Wall -Werror -Wno-sign-compare -Woverloaded-virtual -Wno-deprecated-declarations -Wno-deprecated -Winit-self -Wno-unused-but-set-variable -Wno-psabi -Wno-unknown-pragmas

CXXFLAGS ?= $(ARCHFLAGS) $(INCLUDE) $(GXXWARNINGFLAGS)$(if $(GCC_VERSION_WARNING_FLAGS), $(GCC_VERSION_WARNING_FLAGS),) -pipe -ggdb $(foreach dir,$(LOCAL_INCLUDE_DIR),-I$(dir)) -std=c++1y
CXXNODEBUGFLAGS := -O3 -DBOOST_DISABLE_ASSERTS -DNDEBUG 
CXXDEBUGFLAGS := -O0 -g3

CXXLINKFLAGS = $(PORT_LINK_FLAGS) -rdynamic $(foreach DIR,$(PWD)/$(BIN) $(PWD)/$(LIB),-L$(DIR) -Wl,--rpath-link,$(DIR)) -Wl,--rpath,\$$ORIGIN/../bin -Wl,--rpath,\$$ORIGIN/../lib -Wl,--copy-dt-needed-entries -Wl,--no-as-needed
CXXLIBRARYFLAGS = -shared $(CXXLINKFLAGS) -lpthread
CXXEXEFLAGS =$(CXXLINKFLAGS) -lpthread
CXXEXEPOSTFLAGS = $(if $(MEMORY_ALLOC_LIBRARY),-l$(MEMORY_ALLOC_LIBRARY))



CC ?= $(COMPILER_CACHE) $(GCC)
GCCWARNINGFLAGS?=-Wall -Werror -Wno-sign-compare -fno-strict-overflow -Wno-unused-but-set-variable
CFLAGS ?= $(ARCHFLAGS) $(INCLUDE) $(GCCWARNINGFLAGS) -pipe -O3 -g -DNDEBUG -fno-omit-frame-pointer
CDEBUGFLAGS := -O0 -g
CNODEBUGFLAGS := -O3 -g -DNDEBUG 

FC ?= gfortran
FFLAGS ?= $(ARCHFLAGS) -I. -fPIC

# Extra flags to deal with standard library defects on Ubuntu 14.04
CXXFLAGS += -D_GLIBCXX_USE_NANOSLEEP=1 -D_GLIBCXX_USE_SCHED_YIELD=1

$(if $(findstring x4.8,x$(GCC_VERSION)),$(eval CXXFLAGS += -Wno-unused-local-typedefs -Wno-return-local-addr))
$(if $(findstring x4.9,x$(GCC_VERSION)),$(eval CXXFLAGS += -Wno-unused-local-typedefs))
$(if $(findstring x5.1,x$(CXX_VERSION)),$(eval CXXFLAGS += -Wno-unused-local-typedefs -Wno-unused-variable))

