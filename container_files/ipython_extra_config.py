c = get_config()

c.NotebookApp.ip = '{{IPYTHON_NB_LISTEN_ADDR}}'
c.NotebookApp.port = {{IPYTHON_NB_LISTEN_PORT}}
c.NotebookApp.open_browser = False

c.NotebookApp.notebook_dir = u'{{IPYTHON_NB_DIR}}'
c.NotebookApp.base_url = '{{HTTP_BASE_URL}}/{{IPYTHON_NB_PREFIX}}'
c.NotebookApp.tornado_settings = {'static_url_prefix':'{{HTTP_BASE_URL}}/{{IPYTHON_NB_PREFIX}}/static/'}

# Disable token auth for now
c.NotebookApp.token = ''
c.NotebookApp.password = ''
