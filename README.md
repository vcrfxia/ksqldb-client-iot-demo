# ksqlDB meets Java: An IoT-inspired demo

A simple, IoT-inspired demo application that showcases the [Java client for ksqlDB](https://docs.ksqldb.io/en/latest/developer-guide/ksqldb-clients/java-client/).

Consider an IoT use case where multiple types of sensors each send readings to specific
Kafka topics, one for each sensor type. There may be multiple sensors of each type, all
reporting data to the same topic. Each sensor is identified by a unique sensor ID, and the
data schema for each sensor type is assumed to have a string-type `sensorID` value
column accordingly.

The example `SensorTypesManager`s support registering new sensor types, querying
specific sensors for their latest values, and removing sensor types (and cleaning up the
associated resources). The methods in [`BlockingSensorTypesManager`](src/main/java/my/ksqldb/app/BlockingSensorTypesManager.java)
are blocking while those in [`NonBlockingSensorTypesManager`](src/main/java/my/ksqldb/app/NonBlockingSensorTypesManager.java)
are not, and showcase `CompletableFuture` composition as well.
 
See the associated blog post at [TODO: blog post link] for more.