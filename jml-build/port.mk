$(BUILD)/$(ARCH)/osdeps/tmp/installed-%:
	$(JML_BUILD)/arch/install-port-package.sh $* $(ARCH) $(BUILD)/$(ARCH)/osdeps
	touch $@

PORT_DEPS:=$(foreach package,$(PORT_DEV_PACKAGES_$(port)),$(BUILD)/$(ARCH)/osdeps/tmp/installed-$(package))

port_deps: $(PORT_DEPS)

