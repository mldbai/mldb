# Scaling MLDB

A single instance of MLDB can be run on a laptop for remote or day-to-day data science work, or multiple instances can be run across multiple machines for high-availability production deployments. MLDB exposes an HTTP REST API, so horizontal scaling is just a matter of spinning up more MLDB nodes and putting a load balancer in front of them.

The following diagram shows a typical distributed MLDB deployment. At the center of the diagram is a shared storage system such as Amazon S3 or the Hadoop Distributed File System (HDFS). Individual MLDB nodes in a distributed deployment will read and write data from this shared storage. The three main functions of an MLDB node are:

* Data collection: continuously recording data to shared storage in response to HTTP POST requests
* Scoring: continuously applying machine learning models (defined by files retrieved from shared storage) in response to HTTP GET requests
* Model training & batch application: running long analytic queries and jobs on data from shared storage, and save the results back in shared storage

IMAGE HERE

Data collection and scoring scale horizontally: you can add more nodes to collect data or score faster.

Model training & batch operations scale vertically, then horizontally. Any given model is trained on a single node, so training speed is limited by the CPU of the node and input size is limited by the RAM of the node. That said, multiple nodes can train multiple models in parallel. In a typical high-throughput deployment, a small number of high-memory/high-compute machines handles training.