# Replicate Connector for Kafka

Copyright (C) 2016 Dbvisit Software Limited -- Updated 2 December 2016.

# 1 Introduction

Replicate Connector for Kafka is a [Kafka Connector](https://www.confluent.io/product/connectors/) for loading change records from the REDO logs of an Oracle database, as mined by [Dbvisit Replicate](http://www.dbvisit.com/products/dbvisit_replicate_real_time_oracle_database_replication/), to Kafka.

# 2 Licensing

This software is released under the Apache 2.0 license, a copy of which is located in the LICENSE file.

# 3 Building

You can build the Replicate Connector for Kafka with Maven using the standard lifecycle phases. The Replicate Connector for Kafka requires the [Dbvisit Replicate Connector Library](https://github.com/dbvisitsoftware/replicate-connector-library), which must be built and installed prior to building this connector.

```
mvn clean package
```

To build developer documentation run:

```
mvn javadoc:javadoc
```

# 4 Documentation

User documentation for Replicate Connector for Kafka is located [here](http://replicate-connector-for-kafka.readthedocs.io/en/latest).

# 5 Installation

To install and use the Replicate Connector for Kafka follow the steps outlined in the [Quickstart Guide](http://replicate-connector-for-kafka.readthedocs.io/en/latest/source_connector.html#quickstart).

# 6 Contributions

Active communities contribute code and we're happy to consider yours. To be involved, please email <a href="mailto:github@dbvisit.com">Mike Donovan</a>.
