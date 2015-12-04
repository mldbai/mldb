c = get_config()

c.NotebookApp.ip = '{{IPYTHON_NB_LISTEN_ADDR}}'
c.NotebookApp.port = {{IPYTHON_NB_LISTEN_PORT}}
c.NotebookApp.open_browser = False

c.NotebookApp.notebook_dir = u'{{IPYTHON_NB_DIR}}'
c.NotebookApp.base_url = '{{IPYTHON_NB_BASE_URL}}'
c.NotebookApp.tornado_settings = {'static_url_prefix':'{{IPYTHON_NB_BASE_URL}}/static/'}
