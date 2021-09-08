DEFAULTGXX:=arm64-linux-gnu-g++
DEFAULTGCC:=arm64-linux-gnu-gcc
toolchain?=clang
ARM64_TARGET_OPTION:=$(if $(findstring clang,$(toolchain)),--target=arm64-apple-darwin20)
ARCHFLAGS:=-fPIC -fno-omit-frame-pointer -I$(BUILD)/$(ARCH)/osdeps/usr/include $(ARM64_TARGET_OPTION)

VALGRIND:=
VALGRINDFLAGS:=

TCMALLOC_ENABLED:=0
