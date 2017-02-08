# release.mk
# Jeremy Barnes, 2 September 2015
# Release makefile for MLDB


ifneq ($(PREMAKE),1)

### Container related targets and rules
include mldb/container_files/template_vars.mk
include mldb/templated_files.mk

define mldb_install_directory
$(eval $(call install_directory,$(1),$(2),mldb))
endef

$(eval $(call mldb_install_directory,mldb/container_files/demos,$(ALTROOT)/$(IPYTHON_DIR)))
$(eval $(call mldb_install_directory,mldb/container_files/tutorials,$(ALTROOT)/$(IPYTHON_DIR)))
$(eval $(call mldb_install_directory,mldb/container_files/other,$(ALTROOT)/$(IPYTHON_DIR)))
$(eval $(call mldb_install_directory,mldb/container_files/public_html/doc,$(ALTROOT)/opt/local/public_html))
$(eval $(call mldb_install_directory,mldb/container_files/public_html/resources,$(ALTROOT)/opt/local/public_html))

$(eval $(call install_file,mldb/container_files/init/mldb_logger.py,$(ETC)/service/mldb_runner/log/run,555,mldb))
$(eval $(call install_file,mldb/container_files/init/mldb_finish.py,$(ETC)/service/mldb_runner/finish,555,mldb))
$(eval $(call install_file,mldb/ext/uap-core/regexes.yaml,$(BIN)/useragent-regexes.yaml,644,mldb))
$(eval $(call install_file,mldb/container_files/bashrc,$(ETC)/bash.bashrc,555,mldb))

mldb: \
	$(BIN)/mldb_runner \
	$(BIN)/validator_api.wsgi \
	mldb_plugins

docker_mldb: \
	DOCKER_COMMIT_ARGS=--change='CMD [ "/sbin/my_init","--quiet" ]' --change='EXPOSE 80' --change='VOLUME $(MLDB_DATA_DIR)'

ifdef VERSION_NAME
  # Release related flags and options
	MLDB_EXTRA_FLAGS+= --hide-internal-entities
  DOCKER_POST_INSTALL_ARGS="-s"
endif

docker_mldb: \
	DOCKER_BASE_IMAGE=quay.io/mldb/mldb_base:14.04
	DOCKER_POST_INSTALL_SCRIPT=mldb/container_files/docker_post_install.sh $(DOCKER_POST_INSTALL_ARGS)

mldb_base:
	./mldb/mldb_base/docker_create_mldb_base.sh -w https://wheelhouse.mldb.ai/ubuntu/trusty/x86_64 $(IMG_NAME)
.PHONY: mldb_base

baseimage:
	(cd mldb/baseimage-docker && make build NAME=quay.io/mldb/baseimage)

endif
