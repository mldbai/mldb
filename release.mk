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

define mldb_install_templated_file
$(eval $(call install_templated_file,$(1),$(2),$(3),mldb,mldb/container_files/template_vars.mk,$(J2ENV)))
endef

define mldb_install_directory
$(eval $(call install_directory,$(1),$(2),mldb))
endef

$(eval $(call mldb_install_directory,mldb/container_files/assets,$(ALTROOT)/opt/local))
$(eval $(call mldb_install_directory,mldb/container_files/demos,$(ALTROOT)/$(IPYTHON_DIR)))
$(eval $(call mldb_install_directory,mldb/container_files/tutorials,$(ALTROOT)/$(IPYTHON_DIR)))

$(eval $(call mldb_install_templated_file,mldb/container_files/version.json,$(ALTROOT)/opt/local/version.json))

$(eval $(call mldb_install_templated_file,mldb/container_files/init/05-mldb-id-mapping.sh,$(ETC)/my_init.d/05-mldb-id-mapping.sh,555))
$(eval $(call mldb_install_templated_file,mldb/container_files/init/mldb_runner.sh,$(ETC)/service/mldb_runner/run,555))
$(eval $(call mldb_install_templated_file,mldb/container_files/init/mldb_logger.py,$(ETC)/service/mldb_runner/log/run,555))
$(eval $(call mldb_install_templated_file,mldb/container_files/init/mldb_finish.py,$(ETC)/service/mldb_runner/finish,555))
$(eval $(call mldb_install_templated_file,mldb/container_files/init/nginx_runner.sh,$(ETC)/service/nginx/run,555))
$(eval $(call mldb_install_templated_file,mldb/container_files/mldb_nginx_site.conf,$(ETC)/nginx/sites-enabled/mldb))
$(eval $(call mldb_install_templated_file,mldb/container_files/nginx.conf,$(ETC)/nginx/nginx.conf))
$(eval $(call mldb_install_templated_file,mldb/container_files/init/credentialsd_runner.sh,$(ETC)/service/credentialsd/run,555))
$(eval $(call mldb_install_templated_file,mldb/container_files/init/ipython_notebook_runner.sh,$(ETC)/service/ipython_notebook/run,555))
$(eval $(call mldb_install_templated_file,mldb/container_files/ipython_extra_config.py,$(ALTROOT)/$(IPYTHON_DIR)/ipython_extra_config.py))
$(eval $(call mldb_install_templated_file,mldb/container_files/ipython_custom.js,$(ALTROOT)/$(IPYTHON_DIR)/custom.js))
$(eval $(call mldb_install_templated_file,mldb/container_files/ipython_custom.css,$(ALTROOT)/$(IPYTHON_DIR)/custom.css))
$(eval $(call mldb_install_templated_file,mldb/container_files/init/uwsgi_validator_runner.sh,$(ETC)/service/uwsgi_activator_api/run,555))
$(eval $(call mldb_install_templated_file,mldb/container_files/validator_api_config.json,$(ETC)/validator_api_config.json))
$(eval $(call mldb_install_templated_file,mldb/container_files/publickey.pem,$(ETC)/publickey.pem))
$(eval $(call mldb_install_templated_file,mldb/container_files/classifiers.json,$(BIN)/classifiers.json))

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
