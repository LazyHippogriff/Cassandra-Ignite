# Standalone Cassandra vs Cassandra+Ignite

This repository was created in reference to the stack overflow question: https://stackoverflow.com/q/50094732/5701173

As mentioned in the SO question, I'm comparing the throughput(number of rows read per second) of a table stored in Cassandra in 2 scenarios:

Scenario 1: Read query done via Datastax's C++ driver. The read queries were synchronous i.e. callback function was not used.

Scenario 2: Ignite-C++ added to Cassandra for caching and then the read query is done via Ignite-C++ API.

Currently, the read throughput in both the above scenarios has been as follows:

Scenario 1: 21K rows/second.

Scenario 2: 23K rows/second.

Please note:
Cassandra cluster contains only 1 node. 
No. of processors: 24
Total RAM: 62 GB
Cassandra version: 3.11.2

Also, in the 2nd scenario, Ignite-C++ server is running on top of Cassandra. The client program in both cases also runs on the same node.


I'll be adding the following:

1. Cassandra configurations(cassandra.yaml, cassandra-env.sh, jvm.options)
2. Ignite configurations(config.xml, persistence.xml)
3. Cassandra's client program.
4. Ignite's client program.



