# release.mk
# Jeremy Barnes, 2 September 2015
# Release makefile for MLDB


ifneq ($(PREMAKE),1)

#mldb_release: DOCKER_TAGS+=latest alpha_$(shell git rev-parse HEAD)
mldb_release: RUN_STRIP="-s"
mldb_release: docker_mldb

mldb_push_release: PUSH_TAGS+=latest
mldb_push_release:
	$(if $(PUSH_TAGS),$(foreach tag,$(PUSH_TAGS),docker push $(DOCKER_REGISTRY)$(DOCKER_USER)mldb:$(tag);),@echo PUSH_TAGS empty, nothing to do)


### Container related targets and rules
include mldb/container_files/template_vars.mk
include mldb/templated_files.mk

define mldb_install_directory
$(eval $(call install_directory,$(1),$(2),mldb))
endef

$(eval $(call mldb_install_directory,mldb/container_files/demos,$(ALTROOT)/$(IPYTHON_DIR)))
$(eval $(call mldb_install_directory,mldb/container_files/tutorials,$(ALTROOT)/$(IPYTHON_DIR)))
$(eval $(call install_file,mldb/container_files/init/mldb_logger.py,$(ETC)/service/mldb_runner/log/run,555,mldb))
$(eval $(call install_file,mldb/container_files/init/mldb_finish.py,$(ETC)/service/mldb_runner/finish,555,mldb))

mldb: \
	$(BIN)/mldb_runner \
	$(BIN)/credentialsd \
	$(BIN)/validator_api.wsgi \
	mldb_plugins

docker_mldb: \
	DOCKER_COMMIT_ARGS=--change='CMD [ "/sbin/my_init" ]' --change='EXPOSE 80' --change='VOLUME $(MLDB_DATA_DIR)'

ifneq ($(strip $(RUN_STRIP)),) # overload of the RUN_STRIP meaning here
	MLDB_EXTRA_FLAGS+= --hide-internal-entities
endif
# $(info MLDB_EXTRA_FLAGS are [${MLDB_EXTRA_FLAGS}])
RUN_STRIP := $(if $(or $(STRIP_LIB),$(DOCKER_PUSH)),"-s","")

docker_mldb: \
	DOCKER_BASE_IMAGE=quay.io/datacratic/mldb_base:14.04
	DOCKER_POST_INSTALL_SCRIPT=mldb/container_files/docker_post_install.sh $(RUN_STRIP)

mldb_base:
	./mldb/mldb_base/docker_create_mldb_base.sh -w https://wheelhouse.datacratic.com/public/ubuntu/trusty/x86_64 $(IMG_NAME)
.PHONY: mldb_base

baseimage:
	(cd mldb/baseimage-docker && make build NAME=quay.io/datacratic/baseimage)

endif
