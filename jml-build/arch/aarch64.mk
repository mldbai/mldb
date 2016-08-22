DEFAULTGXX:=aarch64-linux-gnu-g++
DEFAULTGCC:=aarch64-linux-gnu-gcc
ARCHFLAGS:=-fPIC -fno-omit-frame-pointer -I$(BUILD)/$(ARCH)/osdeps/usr/include -I$(BUILD)/$(ARCH)/osdeps/usr/include/aarch64-linux-gnu
PORT_LINK_FLAGS:= -L$(BUILD)/$(ARCH)/osdeps/usr/lib/aarch64-linux-gnu -L$(BUILD)/$(ARCH)/osdeps/usr/lib -Wl,--rpath,$(BUILD)/$(ARCH)/osdeps/usr/lib/aarch64-linux-gnu
TCMALLOC_ENABLED:=0

PORT_DEV_PACKAGES:=libicu52 libicu-dev libpython2.7-dev openssl libssl-dev libboost-program-options1.54.0 libboost-program-options1.54-dev libboost-filesystem1.54 libboost-filesystem1.54-dev libboost-system1.54.0 libboost-system1.54-dev libboost-atomic1.54.0 libboost-program-options1.54-dev libboost-filesystem1.54-dev python2.7 python2.7-dev python2.7-minimal libpython2.7-dev libpython2.7-minimal libcrypto++9 libcrypto++-dev libarchive13 libarchive-dev libpq5 libpq-dev liblzma5 liblzma-dev libyaml-cpp0.5 libyaml-cpp-dev libboost-test1.54.0 libboost-test1.54-dev libboost-locale1.54.0 libboost-locale1.54-dev libboost-thread1.54.0 libboost-thread1.54-dev

$(BUILD)/$(ARCH)/osdeps/tmp/installed-%:
	$(JML_BUILD)/arch/install-port-package.sh $* $(ARCH) $(BUILD)/$(ARCH)/osdeps
	touch $@

PORT_DEPS:=$(foreach package,$(PORT_DEV_PACKAGES),$(BUILD)/$(ARCH)/osdeps/tmp/installed-$(package))

port_deps: $(PORT_DEPS)
