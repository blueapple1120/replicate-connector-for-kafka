Configuration Options
=====================

Configuration Parameters
------------------------

Key User Configuration Properties
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. note::
    **plog.location.uri** - is the location from which Dbvisit Replicate Connector will search for and read PLOG files delivered by the Dbvisit Replicate application. This directory must be local to the filesystem on which Kafka Connect is running, or accessible to it via a mounted filesystem. It cannot be an otherwise remote filesystem.

.. note::
    **plog.data.flush.size** - for low transactional volumes (and for testing) it is best to change the default value of ``plog.data.flush.size`` in the configuration file to a value less than your total number of change records, eg. for manual testing you can use 1 to verify that each and every record is emitted correctly. This configuration parameter is used as the internal PLOG reader's cache size which translates at run time to the size of the polled batch of Kafka messages. It is more efficient under high transactional load to publish to Kafka (particularly in distributed mode where network latency might be an issue) in batches. Please note that in this approach the data from the PLOG reader is only emitted once the cache is full (for the specific Kafka source task) and/or the PLOG is done, so an Oracle redo log switch has occurred. This means that if the ``plog.data.flush.size`` is greater than total number of LCRs in cache it will wait for more data to arrive or a log switch to occur.


All User Configuration Properties
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

**Version 2.8.04**

``tasks.max``
  Maximum number of tasks to start for processing PLOGs.

  * Type: string
  * Default: 4

``topic.prefix``
  Prefix for all topic names.

  * Type: string
  * Default:

``plog.location.uri``
  Replicate PLOG location URI, output of Replicate MINE.

  * Type: string
  * Default: file:/home/oracle/ktest/mine

``plog.data.flush.size``
  LCRs to cache before flushing, for connector this is the batch size, choose this value according to transactional volume, for high throughput to kafka the default value may suffic, for low or sporadic volume lower this value, eg. for testing use 1 which will not use cache and emit every record immediately.

  * Type: string
  * Default: 1000

``plog.interval.time.ms``
  Time in milliseconds for one wait interval, used by scans and health check.

  * Type: string
  * Default: 500

``plog.scan.interval.count``
  Number of intervals between scans, eg. 5 x 0.5s = 2.5s scan wait time.

  * Type: string
  * Default: 5

``plog.health.check.interval``
  Number of intervals between health checks, these are used when initially waiting for MINE to produce PLOGs, eg. 10 * 0.5s = 5.0s.

  * Type: string
  * Default: 10

``plog.scan.offline.interval``
  Default number of health check scans to decide whether or not replicate is offline, this is used as time out value. NOTE for testing use 1, i.e. quit after first health check 1 * 10 * 0.5s = 5s where 10 is plog.health.check.interval value and 0.5s is plog.interval.time.ms value.

  * Type: string
  * Default: 1000

``topic.name.transaction.info``
  Topic name for transaction meta data stream.

  * Type: string
  * Default: TX.META

``plog.global.scn.cold.start``
  Global SCN when to start loading data during cold start.

  * Type: string
  * Default: 0


**Version 2.9.00**
  Includes all configuration options for 2.8.04.

``connector.publish.cdc.format``
  Set the output CDC format of the Replicate Connector. This determines what type of messages are published to Kafka, and the options are:

  1. changerow - complete row, the view of the table record after the action was applied (Default)
  2. changeset - publish the KEY, NEW, OLD and LOB change fields separately

  * Type: string
  * Default: changerow

``connector.publish.transaction.info``
  Set whether or not the transaction info topic (topic.name.transaction.info) should be populated. This includes adding three fields to each Kafka data message to link the individual change message to its parent transaction message:

  * XID - transaction ID
  * TYPE - type of change, e.g. INSERT, UPDATE or DELETE
  * CHANGE_ID - unique ID of change

  The options are:

  1. true
  2. false - do not publish the extra transaction info and fields

  * Type: string
  * Default: true

``connector.publish.keys``
  Set whether or not keys should be published to all table topics. Keys are either primary or unique table constraints. When none of these are available all columns with either character, numeric or date data types are used as the key. The latter is not ideal, so it is encouraged to use PK or unique key constraints on source table.

  The options are:

  1. true - publish key schema and values for all Kafka data messages (not transactional info message)
  2. false - do not publish keys

  * Type: string
  * Default: false

``connector.publish.no.schema.evolution``
  If logical data types are used as default values certain versions of Schema Registry might fail validation due to an issue, see #556. This option is provided for disabling schema evolution for BACKWARDS compatible schemas, effectively forcing all messages to conform to the first schema version published by ignoring all subsequent DDL operations.

  The options are:

  1. true - disable schema evolution, ignore all DDL modifications
  2. false - allow schema evolution for Schema Registry version 3.3 and newer

  * Type: string
  * Default: true

``topic.static.schemas``
  Define the source schemas, as a comma separated list of fully qualified source table names, that may be considered static or only receiving sporadic changes. The committed offsets of their last message can be safely ignored if the lapsed days between the source PLOG of a new message and that of a previous one exceeds topic.static.offsets.age.days

  Example:
  * SCHEMA.TABLE1,SCHEMA.TABLE2

  * Type: string
  * Default: none

``topic.static.offsets.age.days``
  The age of the last committed offset for a static schema topic.static.schemas, when it can be safely ignored during a task restart and stream rewind. A message that originated from a source PLOG older will be considered static and not restart at its original source PLOG stream offset, but instead at its next available message offset. This is intended for static look up tables that rarely change when their source PLOGs may have been flushed since their last update. Defaults to 7 days.

  * Type: string
  * Default: 7   





Data Types
----------

+----------------------+---------------------+------------------+--------------------------------------------------+
| Oracle Data Type     | Connect Data Type   | Default Value    | Conversion Rule                                  |
+======================+=====================+==================+==================================================+
| NUMBER               | Int32               | -1               | scale <= 0 and precision - scale < 10            |
+----------------------+---------------------+------------------+--------------------------------------------------+
| NUMBER               | Int64               | -1L              | scale <= 0 and precision - scale > 10 and < 20   |
+----------------------+---------------------+------------------+--------------------------------------------------+
| NUMBER               | Decimal             | BigDecimal.ZERO  | scale > 0 or precision - scale > 20              |
+----------------------+---------------------+------------------+--------------------------------------------------+
| CHAR                 | Type.String         | Empty string     | Encoded as UTF8 string                           |
|                      |                     | (zero length)    |                                                  |
+----------------------+---------------------+------------------+--------------------------------------------------+
| VARCHAR              | ""                  | ""               | ""                                               |
+----------------------+---------------------+------------------+--------------------------------------------------+
| VARCHAR2             | ""                  | ""               | ""                                               |
+----------------------+---------------------+------------------+--------------------------------------------------+
| LONG                 | ""                  | ""               | ""                                               |
+----------------------+---------------------+------------------+--------------------------------------------------+
| NCHAR                | Type.String         | Empty string     | Encoded as UTF8, attempt is made to auto-detect  |
|                      |                     | (zero length)    | if national character set was UTF-16             |
+----------------------+---------------------+------------------+--------------------------------------------------+
| NVARCHAR             | ""                  | ""               | ""                                               |
+----------------------+---------------------+------------------+--------------------------------------------------+
| NVARCHAR2            | ""                  | ""               | ""                                               |
+----------------------+---------------------+------------------+--------------------------------------------------+
| INTERVAL DAY TO      | Type.String         | Empty string     |                                                  |
| SECOND               |                     | (zero length)    |                                                  |
+----------------------+---------------------+------------------+--------------------------------------------------+
| INTERVAL YEAR TO     | ""                  |  ""              |                                                  |
| MONTH                |                     |                  |                                                  |
+----------------------+---------------------+------------------+--------------------------------------------------+
| CLOB                 | Type.String         | Empty string     | UTF8 string                                      |
|                      |                     | (zero length)    |                                                  |
+----------------------+---------------------+------------------+--------------------------------------------------+
| NCLOB                | ""                  | ""               | ""                                               |
+----------------------+---------------------+------------------+--------------------------------------------------+
| DATE                 | Timestamp           | Epoch time       |                                                  |
+----------------------+---------------------+------------------+--------------------------------------------------+
| TIMESTAMP            | ""                  | ""               |                                                  |
+----------------------+---------------------+------------------+--------------------------------------------------+
| TIMESTAMP WITH TIME  | ""                  | ""               |                                                  |
| ZONE                 |                     |                  |                                                  |
+----------------------+---------------------+------------------+--------------------------------------------------+
| TIMESTAMP WITH LOCAL | ""                  | ""               |                                                  |
| TIME ZONE            |                     |                  |                                                  |
+----------------------+---------------------+------------------+--------------------------------------------------+
| BLOB                 | Bytes               | Empty byte array | Converted from SerialBlob to bytes               |
|                      |                     | (zero length)    |                                                  |
+----------------------+---------------------+------------------+--------------------------------------------------+
| RAW                  | Bytes               | Empty byte array | No conversion                                    |
|                      |                     | (zero length)    |                                                  |
+----------------------+---------------------+------------------+--------------------------------------------------+
| LONG RAW             | ""                  | ""               | ""                                               |
+----------------------+---------------------+------------------+--------------------------------------------------+


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


