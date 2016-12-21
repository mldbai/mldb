# Variables for templating
export GIT_HASH := $(shell git rev-parse HEAD)
export HTTP_BASE_URL :=  # empty. '/' is added in nginx conf
export IPYTHON_DIR = /opt/local/ipython
export IPYTHON_IMAGES_DIR = /usr/local/lib/python2.7/dist-packages/notebook/static/base/images
export IPYTHON_NB_DIR = $(MLDB_DATA_DIR)
export IPYTHON_NB_LISTEN_ADDR = 127.0.0.1
export IPYTHON_NB_LISTEN_PORT = 13020
export IPYTHON_NB_PREFIX = ipy
export MLDB_CREDENTIALS_PATH = file://$(MLDB_DATA_DIR)/.mldb_credentials
export MLDB_DATA_DIR = /mldb_data
#extra flags that varies for release / debug build are specified in release.mk
export MLDB_EXTRA_FLAGS = # passed as-is to the mldb_runner executable
export MLDB_GLOBAL_PLUGINS_PATH = file:///opt/mldb/plugins
export MLDB_LOCAL_PLUGINS_PATH = file:///mldb_data/plugins/autoload
export MLDB_LOGFILE = /var/log/mldb_runner.log
export MLDB_LOGGER_HTTP_PORT = 13500
export MLDB_PUBLIC_HTML_PATH = /opt/local/public_html
export MLDB_RUNNER_LISTEN_ADDR = 127.0.0.1
export MLDB_RUNNER_LISTEN_PORT = 11700
export MLDB_SSD_CACHE_PATH = /ssd_cache
export MLDB_USER = _mldb
export MLDB_VALIDATOR_DIR = $(MLDB_DATA_DIR)/.mldb_validator
export NGINX_ROOT = /opt/local/public_html/nginx
export UWSGI_VALIDATOR_PORT = 9100
export VERSION_NAME2 = $(if $(VERSION_NAME),$(VERSION_NAME),$(shell date +"%Y.%m.%d.dev-")$(shell git rev-parse --short HEAD))
