DEFAULTGXX:=arm64-linux-gnu-g++
DEFAULTGCC:=arm64-linux-gnu-gcc
toolchain?=clang
ARCHFLAGS:=-fPIC -fno-omit-frame-pointer -I$(BUILD)/$(ARCH)/osdeps/usr/include --target=arm64-apple-darwin20

VALGRIND:=
VALGRINDFLAGS:=

TCMALLOC_ENABLED:=0
