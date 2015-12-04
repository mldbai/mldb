# Variables for templating
export MLDB_DATA_DIR = /mldb_data
export IPYTHON_NB_LISTEN_ADDR = 127.0.0.1
export IPYTHON_NB_LISTEN_PORT = 13020
export IPYTHON_NB_DIR = $(MLDB_DATA_DIR)
export IPYTHON_DIR = /opt/local/ipython
export IPYTHON_NB_BASE_URL = /ipy
export IPYTHON_IMAGES_DIR = /usr/local/lib/python2.7/dist-packages/notebook/static/base/images
export UWSGI_VALIDATOR_PORT = 9100 
export CREDENTIALSD_LISTEN_PORT = 13011
export CREDENTIALSD_LISTEN_ADDR = 127.0.0.1
export CREDENTIALSD_BASE_URL = /v1/creds
export MLDB_RUNNER_LISTEN_ADDR = 127.0.0.1
export MLDB_RUNNER_LISTEN_PORT = 11700
export MLDB_STATIC_ASSETS_PATH = /opt/local/assets
export MLDB_CONFIGURATION_PATH = file://$(MLDB_DATA_DIR)/.mldb_config
export MLDB_CREDENTIALS_PATH = file://$(MLDB_DATA_DIR)/.mldb_credentials
export MLDB_VALIDATOR_DIR = $(MLDB_DATA_DIR)/.mldb_validator
export VERSION_NAME2 = $(VERSION_NAME)
export GIT_HASH := $(shell git rev-parse HEAD)
export MLDB_SSD_CACHE_PATH = /ssd_cache
export MLDB_EXTRA_FLAGS =  # these flags will be passed as-is to the mldb_runner executable
export MLDB_GLOBAL_PLUGINS_PATH = file:///opt/mldb/plugins
export MLDB_LOCAL_PLUGINS_PATH = file:///mldb_data/plugins/autoload


