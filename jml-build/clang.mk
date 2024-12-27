# Note: clang 3.6 won't compile MLDB if GCC6 is installed

SPACE:=$(NOTHING) $(NOTHING)
make_cacheable2=$(subst =,X,$(subst $(SPACE),^,$(1)))
make_cacheable=$(w arning make_cacheable $(1))$(call make_cacheable2,$(1))
find_command_impl=$(foreach command,$(firstword $(1)),$(if $(call exec-shell, which $(command)),$(command),$(call find_command,$(wordlist 2,10000,$(1)))))
find_command=$(w arning find_command $(1) var=__find_command_cache_$(call make_cacheable,$(1)))$(if $(__find_command_cache_$(call make_cacheable,$(1))),$(__find_command_cache_$(call make_cacheable,$(1))),$(eval __find_command_cache_$(call make_cacheable,$(1)):=$(call find_command_impl,$(1)))$(__find_command_cache_$(call make_cacheable,$(1))))


ifeq ($(CLANG),)
CLANG:=$(call find_command,clang clang-19 clang-18 clang-17 clang-16 clang-15 clang-14 clang-13 clang-12 clang-11 clang-10 clang-9 clang-8)
endif

ifeq ($(CLANGXX),)
CLANGXX:=$(call find_command,clang++ clang++-19 clang++-18 clang++-17 clang++-16 clang++-15 clang++-14 clang++-13 clang++-12 clang++-11 clang++-10 clang++-9 clang++-8)
endif

CLANG_VERSION:=$(call exec-shell, $(CLANGXX) --version | head -n1 | awk '{ print $$4; }' | sed 's/-*//g')
#$(warning CLANG_VERSION=$(CLANG_VERSION))

ifeq ($(CLANG_VERSION_MAJOR),)
CLANG_VERSION_MAJOR:=$(strip $(call exec-shell, $(CLANGXX) -dM -E - < /dev/null | grep __clang_major__ | sed 's/.* //g'))
endif

#$(warning CLANG_MAJOR=$(CLANG_VERSION_MAJOR))

ifeq ($(CXX_VERSION),)
CXX_VERSION:=$(CLANG_VERSION)
endif

CXX := $(COMPILER_CACHE) $(CLANGXX)
BUILDING_WITH_CLANG:=1

#$(warning building with clang++ version $(CLANG_VERSION))

CLANGXXWARNINGFLAGS?=-Wall -Werror -Wno-sign-compare -Woverloaded-virtual -Winit-self -Qunused-arguments -ftemplate-backtrace-limit=0

CLANG_SANITIZER_COMMON_FLAGS:=
CLANG_SANITIZER_address_FLAGS:=-fsanitize=address ${CLANG_SANITIZER_COMMON_FLAGS}
CLANG_SANITIZER_memory_FLAGS:=-fsanitize=memory ${CLANG_SANITIZER_COMMON_FLAGS}
CLANG_SANITIZER_thread_FLAGS:=-fsanitize=thread ${CLANG_SANITIZER_COMMON_FLAGS}
CLANG_SANITIZER_undefined_FLAGS:=-fsanitize=undefined -fno-sanitize-recover=undefined -fno-sanitize=vptr -fvisibility=default --rtlib=compiler-rt ${CLANG_SANITIZER_COMMON_FLAGS}

COMMA?=,
NOTHING?=
SPACE?=$(NOTHING) $(NOTHING)
SANITIZERS:=$(subst $(COMMA),$(SPACE),$(sanitizers))

CXXFLAGS ?= $(ARCHFLAGS) $(INCLUDE) $(CLANGXXWARNINGFLAGS) -fcolor-diagnostics -pipe -ggdb $(foreach dir,$(LOCAL_INCLUDE_DIR),-I$(dir)) -std=c++20 $(foreach sanitizer,$(SANITIZERS),$(CLANG_SANITIZER_$(sanitizer)_FLAGS) )
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
