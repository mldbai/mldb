# MLDB Release Checklist

1. [optional: release a new `pymldb` to pypi if necessary](#pymldb)
1. [Get ready and build a Release Candidate](#ready)
1. [QA your release candidate](#qarc)
1. [tag and push to quay.io, Github](#tag)
1. [build the VMs](#vms)
1. [concurrently, build the documentation](#docs)
1. [push: VMs, `latest` to quay.io, documentation](#push)
1. [publish release notes](#notes)
1. [optional: redeploy the plugin registry if necessary](#plugins)

## <a name="ready"></a> Building a Release Candidate

1. check JIRA and Github for issues closed/commits since last release
 * use this to build up the release notes to be used below
 * ensure that the documentation is up to date
 * optionally ensure that the demos and tutorials are updated to show off the latest functionality
1. ensure you have a clone of code to be released and that it's committed and pushed and that the tests pass
 * ideally ensure that the `mldb` submodule is as up to date as possible
1. record the version number as `<YYYY.DD.MM.X>` using today's date starting with X=0 
  ```
  export mldb_version=`date +"%Y.%m.%d.0"`
  echo $mldb_version
  ```
  
1. build a release candidate container and record the image hash:
  ```
  nice make -j$(nproc) docker_mldb VERSION_NAME=$mldb_version && \
    export mldb_hash=$(docker images -q quay.io/mldb/mldb:$(whoami)_latest) && \
    echo $mldb_hash
  ```

## <a name="qarc"></a> QAing a Release Candidate

1. launch the container (choose an available port instead of 12349, and make sure mldb_data is empty):
  ```
  mkdir mldb_data_${mldb_hash} && \
    docker run --name=mldb_rc_${mldb_hash} --rm=true -v $PWD/mldb_data_${mldb_hash}:/mldb_data -e  MLDB_IDS="`id`"  -p 12349:80 quay.io/mldb/mldb:$mldb_hash
  ```

1. activate the container, ensuring that the activation process works
1. click around the UI, ensuring that it works
1. click around the documentation, ensuring that it all looks ok
1. try calling a few routes in the REST documentation
1. run all the notebooks, ensuring no failures, by running the following command from a different shell:
  ```
  docker exec <container_id> bash /opt/local/ipython/run_notebooks.sh
  ```
  
1. load all plugins, walk through basic functionality
1. ctl-c to kill the container


## <a name="tag"></a> Tagging and Pushing to quay.io, Github

1. from the same terminal where `$mldb_version` is defined, and where you have killed your container
1. docker-tag and push your image: 
  ```
  docker tag $mldb_hash quay.io/mldb/mldb:v$mldb_version
  docker push quay.io/mldb/mldb:v$mldb_version
  ```
  
1. git-tag and push your commit in both `mldb` and `mldbpro`:
  ```
  git tag v$mldb_version
  git push origin v$mldb_version
  ```

## <a name="vms"></a> Building new VMs

1. (this step is generally done by asking Marc to do it on HipChat)
1. Follow [Packer instructions](../container_provis/README.md)
1. Don't push the AMIs or change the main mldb.ova file, but do QA them

## <a name="docs"></a> Rebuilding documentation

1. ensure the container is running at `<host:port>` mapped to `<mldb_data>` (i.e. relaunch the container you QAed in the terminal where `$mldb_version` is defined)
1. ensure that you've run `make python_dependencies` in your `<mldbpro>` directory
1. ensure that the documentation available at `<host:port>/doc` looks good for export
1. ensure that all demo notebooks saved in `<mldb_data>` have been run without errors (see QAing step above) and look good for export
1. in a separate terminal, extract the documentation and notebooks to HTML:

  ```
  cd <mldbpro>/scrape_docs
  source <mldbpro>/virtualenv/bin/activate
  ./scrape_docs.sh <host:port> <mldb_data>
  deactivate
  ```

1. Check that the notebooks were generated:

  ```
  ls <mldbpro>/scrape_docs/build/ipy/notebooks/_*/_latest/*.html
  ```

## <a name="push"></a> Pushing the release

1. switch back to the terminal where `$mldb_version` is defined, killing any running containers
1. Once the VMs have been QA'ed you can proceed (this usually involved downloading the OVA and running the Titanic demo)
1. (ask Marc to) Push the VMs: follow [Packer instructions](../container_provis/README)
1. Move the `latest` docker tag to the current release and push that:

  ```
  docker tag quay.io/mldb/mldb:v$mldb_version quay.io/mldb/mldb:latest
  docker push quay.io/mldb/mldb:latest
  ``` 
  
1. Move the `release_latest` git branch pointer in the `mldb` repo to the current release and push that:

  ```
  cd <mldb>
  git checkout release_latest
  git merge v$mldb_version
  git push
  ``` 

1. Push your built documentation: 

  ```
  cd <mldbpro>/scrape_docs/build
  ```

Start by doing a dry-run of the sync command. Inspect the list of changes to determine if it makes sense considering the changes that have been made to MLDB:
  
  ```
  s3cmd sync --dry-run --delete-removed index.html version.json doc v1 resources ipy s3://docs-mldb-ai/
  ```
  
If all looks good, do the sync.
  
  ```
  s3cmd sync --delete-removed index.html version.json doc v1 resources ipy s3://docs-mldb-ai/
  ```


## <a name="notes"></a> Release Notes

1. switch back to the terminal where `$mldb_version` is defined, killing any running containers
1. ensure clone of https://github.com/mldbai/mldbai.github.io is at `<repo>` and it's up to date
1. Write out some release notes in Markdown
1. Add a blog post announcing the release, including the Markdown notes, by creating a file under `_posts`
  ```
  cd <repo>/_posts
  export last_post=`ls --color=never | grep --color=never version | tail -1`
  export new_post=`date +"%Y-%m-%d-version-$mldb_version.md"`
  echo $last_post $new_post
  cat  $last_post > $new_post
  vim $new_post
  ```

1. run `jekyll serve` locally to QA the site you're about to push
1. commit your changes

  ```
  cd <repo>
  git add _posts
  git commit -am "v$mldb_version release"
  git push
  ```
1. Go to http://blog.mldb.ai to QA the site you just pushed (Github may take a minute to regenerate the site)
1. Go to Github and add the release notes to the Releases tab for both `mldb` and `mldbpro` for the tags you pushed using the Markdown notes


## <a name="pymldb"></a> Pushing to `pymldb` to pypi

1. Jean is the point of contact for these instructions
1. From the `mldbpro` repo, update the version number in `pymldb/version.py`
1. Commit and push your changes to the `pymldb` repo and the `mldbpro` repo, and the `mldb` repo something like:

  ```bash
  cd pymlb
  git commit -am "version bump"
  git push
  cd ..
  git add pymldb
  git commit -m "bumping submodule"
  git push
  ```

1. ensure you've run `make python_dependencies` recently
1. Build the source package, from the root of the `mldb` repo:

   ```bash
   make pymldb_pkg
   ```
   
1. Upload the resulting package to PyPI:

   ```bash
   make pymldb_upload
   ```

1. go into the `mldb` project and update the `pymldb` version in `python_requirements_mldb_base.txt` and `python_constraints.txt`
1. [rebuild and push `mldb_base`](https://github.com/mldbai/mldb/blob/master/Building.md#build-instructions)

## <a name="plugins"></a> Redeploy the Plugin Registry

The plugin registry is currently maintained in the `registry.json` file,
which lives in a git repo: `github.com:mldbai/plugin-registry`.

To modify the registry, update `registry.json` and merge your changes in the `prod` branch.

To redeploy the registry, do as a regular API deployment:
```
# From saltmaster
salt-ssh 'api1-hub.mldb.ai' state.sls products.mldbhub.api
# (continue with api2 and api3)
```

To deploy manually in case of an emergency:
```
ssh api[123]-hub.mldb.ai
cd /opt/hub/plugin_registry
sudo -u hub git fetch
sudo -u hub git checkout origin/prod
```
