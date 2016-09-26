# Command to install a dependency for the given architecture
$(BUILD)/$(ARCH)/osdeps/tmp/installed-%:
	$(JML_BUILD)/arch/install-port-package.sh $* $(ARCH) $(BUILD)/$(ARCH)/osdeps
	touch $@

# Command to identify a dependency's dependencies
$(BUILD)/$(ARCH)/osdeps/tmp/deps-%.mk:
	@mkdir -p $(dir $@)
	$(JML_BUILD)/arch/get-port-package-deps.sh $* $(ARCH) > $@~
	mv $@~ $@

PORT_BLACKOUT_PACKAGES:=$(PORT_BLACKOUT_PACKAGES_$(port))

define do_port_dev_package
-include $(BUILD)/$(ARCH)/osdeps/tmp/deps-$(1).mk
$(BUILD)/$(ARCH)/osdeps/tmp/deps-$(1).mk:	$$(foreach dep,$$(PACKAGE_DEPS_$(1)),$(BUILD)/$(ARCH)/osdeps/tmp/deps-$$(dep).mk)
endef

$(foreach package,$(PORT_DEV_PACKAGES_$(port)),$(eval $(call do_port_dev_package,$(package))))

PORT_ALL_DEPS2:=$(filter-out $(PORT_BLACKOUT_PACKAGES_$(port)),$(sort $(PORT_ALL_DEPS_COMPUTED)))

$(foreach package,$(PORT_ALL_DEPS2),$(eval $(call do_port_dev_package,$(package))))

PORT_ALL_DEPS3:=$(filter-out $(PORT_BLACKOUT_PACKAGES_$(port)),$(sort $(PORT_ALL_DEPS_COMPUTED)))

$(foreach package,$(PORT_ALL_DEPS3),$(eval $(call do_port_dev_package,$(package))))

PORT_ALL_DEPS4:=$(filter-out $(PORT_BLACKOUT_PACKAGES_$(port)),$(sort $(PORT_ALL_DEPS_COMPUTED)))

$(foreach package,$(PORT_ALL_DEPS4),$(eval $(call do_port_dev_package,$(package))))

PORT_ALL_DEPS5:=$(filter-out $(PORT_BLACKOUT_PACKAGES_$(port)),$(sort $(PORT_ALL_DEPS_COMPUTED)))

$(foreach package,$(PORT_ALL_DEPS5),$(eval $(call do_port_dev_package,$(package))))


PORT_DEPS4:=$(foreach package,$(PORT_ALL_DEPS_COMPUTED),$(BUILD)/$(ARCH)/osdeps/tmp/installed-$(package))

port_deps: $(PORT_DEPS4)

