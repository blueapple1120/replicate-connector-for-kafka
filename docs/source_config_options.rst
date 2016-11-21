Configuration Options
=====================

Configuration Parameters
------------------------

Key User Configuration Properties
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

**plog.location.uri**
    This is the location from which Dbvisit Replicate Connector will search for and read PLOG files delivered by the Dbvisit Replicate application. This directory must be local to the filesystem on which Kafka Connect is running, or accessible to it via a mounted filesystem. It cannot be an otherwise remote filesystem.

**plog.data.flush.size**
    For low transactional volumes (and for testing) it's best to change the default value of plog.data.flush.size in the configuration file to a value less than your total number of change records, eg. for manual testing I tend to use 1 to verify that each record is emitted correctly. This configuration parameter is used as the internal PLOG reader's cache size which translates at run time to the size of the polled batch of kafka messages. The idea is that it's more efficient under high transactional load to publish to kafka (particularly in distributed mode where network latency might be an issue) in batches. Please note the caveat of this approach is that the data from PLOG reader is only emitted once the cache is full (for the specific kafka source task) and/or the PLOG is done, so log switch has occurred. This means that if the plog.data.flush.size is greater than total number of LCRs in cache it will wait for more data to arrive or a log switch to occur.


All User Configuration Properties
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

``tasks.max``
  Maximum number of tasks to start for processing PLOGs.

  * Type: string
  * Importance: high
  * Default: 4``

``topic.prefix``
  Prefix for all topic names.

  * Type: string
  * Default: REP-
  * Importance: high``

``plog.location.uri``
  Replicate PLOG location URI, output of Replicate MINE.

  * Type: string
  * Default: file:/home/oracle/ktest/mine
  * Importance: high``

``plog.data.flush.size``
  LCRs to cache before flushing, for connector this is the batch size, choose this value according to transactional volume, for high throughput to kafka the default value may suffic, for low or sporadic volume lower this value, eg. for testing use 1 which will not use cache and emit every record immediately.

  * Type: string
  * Default: 1000
  * Importance: high``

``plog.interval.time.ms``
  Time in milliseconds for one wait interval, used by scans and health check.

  * Type: string
  * Default: 500
  * Importance: high``

``plog.scan.interval.count``
  Number of intervals between scans, eg. 5 x 0.5s = 2.5s scan wait time.

  * Type: string
  * Default: 5
  * Importance: high``

``plog.health.check.interval``
  Number of intervals between health checks, these are used when initially waiting for MINE to produce PLOGs, eg. 10 * 0.5s = 5.0s.

  * Type: string
  * Default: 10
  * Importance: high``

``plog.scan.offline.interval``
  Default number of health check scans to decide whether or not replicate is offline, this is used as time out value. NOTE for testing use 1, i.e. quit after first health check 1 * 10 * 0.5s = 5s where 10 is plog.health.check.interval value and 0.5s is plog.interval.time.ms value.

  * Type: string
  * Default: 1000
  * Importance: high``

``topic.name.transaction.info``
  Topic name for transaction meta data stream.

  * Type: string
  * Default: TX.META
  * Importance: high``

``plog.global.scn.cold.start``
  Global SCN when to start loading data during cold start.

  * Type: string
  * Default: 0
  * Importance: high``


Data Types
----------

+----------------------+---------------------+--------------------------------------------------+
| Oracle Data Type     | Connect Data Type   | Description                                      |
+======================+=====================+==================================================+
| NUMBER               | Int32               | scale <= 0 and precision - scale < 10            |
+----------------------+---------------------+--------------------------------------------------+
| NUMBER               | Int64               |  scale <= 0 and precision - scale > 10 and < 20  |                                    
+----------------------+---------------------+--------------------------------------------------+
| NUMBER               | Decimal             | scale > 0 or precision - scale > 20              |
+----------------------+---------------------+--------------------------------------------------+
| CHAR                 | String              | UTF8 string                                      |
+----------------------+---------------------+--------------------------------------------------+
| VARCHAR              | String              | UTF8 string                                      |
+----------------------+---------------------+--------------------------------------------------+
| VARCHAR2             | String              | UTF8 string                                      |
+----------------------+---------------------+--------------------------------------------------+
| NCHAR                | String              | UTF8, attempt is made to auto-detect if national | 
|                      |                     | character set was UTF-16                         |           
+----------------------+---------------------+--------------------------------------------------+
| NVARCHAR             | String              | UTF8, attempt is made to auto-detect if national | 
|                      |                     | character set was UTF-16                         |           
+----------------------+---------------------+--------------------------------------------------+
| NVARCHAR2            | String              | UTF8, attempt is made to auto-detect if national | 
|                      |                     | character set was UTF-16                         |           
+----------------------+---------------------+--------------------------------------------------+
| LONG                 | String              | UTF8 string                                      |
+----------------------+---------------------+--------------------------------------------------+
| INTERVAL DAY TO      | String              | UTF8 string                                      |
| SECOND               |                     |                                                  |
+----------------------+---------------------+--------------------------------------------------+
| INTERVAL YEAR TO     | String              | UTF8 string                                      |
| MONTH                |                     |                                                  |
+----------------------+---------------------+--------------------------------------------------+
| CLOB                 | String              | UTF8 string                                      |
+----------------------+---------------------+--------------------------------------------------+
| NCLOB                | String              | UTF8 string                                      |
+----------------------+---------------------+--------------------------------------------------+
| DATE                 | Timestamp           |                                                  |
+----------------------+---------------------+--------------------------------------------------+
| TIMESTAMP            | Timestamp           |                                                  |
+----------------------+---------------------+--------------------------------------------------+
| TIMESTAMP WITH TIME  | Timestamp           |                                                  |
| ZONE                 |                     |                                                  |
+----------------------+---------------------+--------------------------------------------------+
| TIMESTAMP WITH LOCAL | Timestamp           |                                                  |
| TIME ZONE            |                     |                                                  |
+----------------------+---------------------+--------------------------------------------------+
| BLOB                 | Bytes               |                                                  |
+----------------------+---------------------+--------------------------------------------------+
| RAW                  | Bytes               |                                                  |
+----------------------+---------------------+--------------------------------------------------+
| LONG RAW             | Bytes               |                                                  |
+----------------------+---------------------+--------------------------------------------------+


Distributed Mode Settings
-------------------------

Use the following to start Dbvisit Replicate Connector for Kafka in Distributed mode, once the Kafka Connect worker has been started on the host node. `Postman <https://www.getpostman.com/>`_ is an excellent utility for working with cUrl commands.

.. sourcecode:: bash

    ➜ curl -v -H "Content-Type: application/json" -X PUT 'http://localhost:8083/connectors/kafka-connect-dbvisitreplicate/config' -d 
  '{
    "connector.class": "com.dbvisit.replicate.kafkaconnect.ReplicateSourceConnector",
    "tasks.max": "2", 
    "topic.prefix": "REP-", 
    "plog.location.uri": "file:/foo/bar",
    "plog.data.flush.size": "1",
    "plog.interval.time.ms": "500",
    "plog.scan.interval.count": "5",
    "plog.health.check.interval": "10",
    "plog.scan.offline.interval": "1000",
    "topic.name.transaction.info": "TX.META"
  }'


Or save this to a file <json_file>:

.. sourcecode:: bash

  {
    "name": "TSource",
    "config": {
      "connector.class": "com.dbvisit.replicate.kafkaconnect.ReplicateSourceConnector",
    "tasks.max": "2", 
    "topic.prefix": "REP-", 
    "plog.location.uri": "file:/foo/bar",
    "plog.data.flush.size": "1",
    "plog.interval.time.ms": "500",
    "plog.scan.interval.count": "5",
    "plog.health.check.interval": "10",
    "plog.scan.offline.interval": "1000",
    "topic.name.transaction.info": "TX.META"
    }
  }

  ➜ curl -X POST -H "Content-Type: application/json" http://localhost:8083 --data "@<json_file>"


