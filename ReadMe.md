# MLDB is the Machine Learning Database
### by [Datacratic](http://datacratic.com/)

MLDB is a new kind of database, one that is specifically designed for operationalizing machine learning. You install it wherever you want and send it commands over a RESTful API to store and explore data, train machine learning models on that data, and then expose those models as APIs. More information is available at http://mldb.ai

This repository contains the source code for MLDB, which can be used to [build the MLDB Community Edition](Building.md), which is a Docker image containing all of MLDB's dependencies and can be run anywhere.

**To get started faster** you can also download a pre-built binary of the [MLDB Enterprise Edition](http://mldb.ai/doc/#builtin/Running.md.html) and use it with a free trial license for non-commercial purposes.

Please [create a Github Issue](https://github.com/mldbai/mldb/issues/new) if you have any questions or encounter problems while building or running MLDB. 

## Documentation

Raw Markdown documentation files are located under `container_files/public_html/doc` and you can browse them on Github or you can browse the full-rendered version at [http://mldb.ai/doc](http://mldb.ai/doc).

## Copyright & License (Apache License v2.0)

MLDB is © 2015 Datacratic, and is distributed under the [Apache License, version 2.0](LICENSE), except for the contents of the `ext` directory, which contains modified versions of other open-source software components, each of which is distributed under its own, Apache-compatible license and lists its own copyright information.
