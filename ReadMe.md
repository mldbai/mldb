# MLDB is the Machine Learning Database

MLDB is an open source SQL database designed for machine learning that was developed by [MLDB.ai](http://mldb.ai/).
Since the sale of MLDB.ai to [Element AI](http://elementai.com) in 2017, it's no longer
a commercially supported product, instead it's being developed by a very small number
of people in their spare time as an open source research project.

*The former MLDB Enterprise Edition, the MLDB Docker Containers, and the MLDB Hub
are no longer being maintained.  Please don't use them.*

[![Join the chat at https://gitter.im/mldbai/mldb](https://badges.gitter.im/mldbai/mldb.svg)](https://gitter.im/mldbai/mldb?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

MLDB is an open-source database designed for machine learning. 
You can install it as a command-line tool wherever you want and either run as a script or send it commands over a RESTful API to 
store data, explore it using SQL, then train machine learning models and expose them as APIs. More information is available at http://mldb.ai

This repository contains the source code for MLDB, which can be used to [build MLDB](Building.md).  Building MLDB is the
_only_ way to get an up to date version.  It can be built and run on Linux or OSX, on Intel, ARM or Apple processors.  The
CI/CD pipeline is hosted on [GitLab](https://gitlab.com/mldbai/mldb/).

Please [create a Github Issue](https://github.com/mldbai/mldb/issues/new) or [chat with us on Gitter](https://gitter.im/mldbai/mldb) if you have any questions or encounter problems while building or running MLDB.  Be mindful that it's open source and that everyone working on it has
a day job.

## What's interesting about MLDB?

MLDB contains some interesting concepts:
* A dialect of SQL that is useful for machine learning
* High efficiency implementations of data loading, training of classical ML algorithms and predition endpoints
* Dataset abstractions that can effectivly model many kinds of real-world data (tabular, sparse, behavioral, logfiles, ...)
* A data model and type system designed for ML, including nested structures, embeddings and tensors as data types
* Everything-is-a-table, allowing manipulation and introspection of ML models
* Lock-free and high performance REST endpoints
* Extensibility via plugins, in C++, Python and Javascript

It is used to explore the following research topics:
* High memory efficient data storage
* High speed training of ML algorithms
* Memory mappable data structures
* Abstractions for compute-independent processing

Currently, MLDB is being rearchitected as a much smaller core with all of the other functionality implemented as plugins,
and designed to run on a broader set of deployment platforms.

The ultimate vision for MLDB is as a machine-learning "anti-plaform": MLDB will make it easy to create
and deploy machine learning solutions by allowing them to be manipulated and transformed outside of the
platforms on which they are created and specialized to their runtime environment.

## Documentation

Raw Markdown documentation files are located under `container_files/public_html/doc` and you can browse them on Github or you can browse the full-rendered version at https://docs.mldb.ai.  This documentation is for the last commercial release, and so is out of date, but is still generally helpful.

## Copyright & License (Apache License v2.0)

MLDB is © 2016 mldb.ai Inc (and its successors) and the Contributors, and is distributed under the [Apache License, version 2.0](LICENSE), except for the contents of the `ext` directory, which contains (possibly) modified versions of other open-source software components, each of which is distributed under its own, Apache-compatible license and lists its own copyright information.  Source code of each component is available via its Git submodule, and any changes to those components in the `mldbai` GitHub organization are implicitly available under the same license as the modified work.
