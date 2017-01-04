
# Variables to be overriden by template_vars.mk via jinja
# watch the string quoting.

RUNAS = "{{MLDB_USER}}"
HTTP_LISTEN_PORT = {{MLDB_LOGGER_HTTP_PORT}}

