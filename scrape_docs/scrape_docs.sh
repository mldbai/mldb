#!/bin/bash

set -u
set -e

# ensure a container is running at <host:port>
# ensure the notebooks are up to date under <mldb_data>
# usage ./scrape_docs.sh <host:port> <mldb_data>

# erase old docs, create new file structure
rm -rf build
mkdir -p build
cp -LR index.html build

curl $1/version.json -o build/version.json

# scrape all the symlinked files, appending .html to .md files
TOSCRAPE="doc v1 resources"

find -L $TOSCRAPE -type f -name "*md" \
| xargs -I % curl $1/%.html -o build/%.html --create-dirs

find -L $TOSCRAPE -type f -not -name "*md" \
| xargs -I % curl $1/% -o build/% --create-dirs

# now convert the notebooks to HTML
for nbdir in _demos _tutorials _other
do
    mkdir -p build/ipy/notebooks/$nbdir/_latest
    jupyter nbconvert $2/$nbdir/_latest/*ipynb --template mldb.tpl --output-dir build/ipy/notebooks/$nbdir/_latest
done
