# MLDB Pro

MLDB Pro is a commercially-available MLDB plugin created by [Datacratic](http://datacratic.com/). It adds to MLDB a number of [dataset types](datasets/Datasets.md) which are faster and more memory-efficient than the ones available under an open-source license as part of MLDB:

The ![](%%doclink beh.mutable dataset) is the most useful dataset type provided by the MLDB Pro plugin. It is a drop-in replacement for the ![](%%doclink sparse.mutable dataset) and can make computations 2-10 times faster. It is a write-once sparse mutable dataset which creates a memory-mappable file of type `.beh` which can then be loaded up immutably with the ![](%%doclink beh dataset). 

The ![](%%doclink beh.binary dataset) and ![](%%doclink beh.binary.mutable dataset) have a similar relationship, but are even more efficient because they only store the cell value `1`.

The MLDB Pro Plugin is part of the [MLDB Enterprise Edition](Running.md), which is freely available under the [MLDB End User License Agreement, Non-Commercial Software License version 1.0](/resources/MLDB_License.pdf). A license permitting commercial use can be purchased from Datacratic by <a href="mailto:mldb@datacratic.com" target="_blank">contacting sales</a>.